// Command reshard 是分片/分库扩缩容辅助工具：
//  1. 计划（默认 dry-run）：给定旧/新分片数与策略，统计需迁移的角色比例与 from→to 分布。
//  2. 执行（-execute，需 PG）：在旧/新 Postgres 分库布局间迁移 roles 行。
//
// 取模（modulus）在分片数变化时几乎全量迁移；一致性哈希（ring）仅迁移约 1/N。
// 建议生产用 ring，并在低峰期分批迁移。
//
// 示例：
//
//	# 规划：50→64 分片，对比取模与一致性哈希迁移量
//	go run ./tools/reshard -old 50 -new 64 -strategy ring -range-end 200000
//
//	# 执行：按取模在两套 PG 分库间迁移（先 dry-run 确认）
//	go run ./tools/reshard -old 2 -new 3 -strategy modulus \
//	  -pg-old "dsn0,dsn1" -pg-new "dsn0,dsn1,dsn2" -execute
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"newgame/pkg/db"
	"newgame/pkg/shard"
)

func main() {
	old := flag.Int("old", 50, "旧分片/分库数量")
	newCount := flag.Int("new", 64, "新分片/分库数量")
	strategy := flag.String("strategy", "ring", "路由策略：ring | modulus")
	replicas := flag.Int("replicas", 200, "一致性哈希每节点虚拟节点数")
	rangeStart := flag.Int64("range-start", 0, "规划模式：role_id 起始（含）")
	rangeEnd := flag.Int64("range-end", 100000, "规划模式：role_id 结束（不含）")
	pgOld := flag.String("pg-old", "", "旧 PG 分库 DSN（逗号分隔），执行模式必填")
	pgNew := flag.String("pg-new", "", "新 PG 分库 DSN（逗号分隔），执行模式必填")
	execute := flag.Bool("execute", false, "执行实际数据迁移（默认仅规划 dry-run）")
	deleteSource := flag.Bool("delete-source", false, "校验目标行后删除源行；默认 copy-only")
	batch := flag.Int("batch", 500, "执行模式每批角色数")
	flag.Parse()

	router := newRouter(*strategy, *old, *newCount, *replicas)

	if *execute {
		if err := runExecute(*pgOld, *pgNew, router, *batch, *deleteSource); err != nil {
			fmt.Fprintln(os.Stderr, "execute error:", err)
			os.Exit(1)
		}
		return
	}
	runPlan(router, *old, *newCount, *strategy, *rangeStart, *rangeEnd)
}

// router 抽象「role_id → 节点索引」的新旧映射。
type router struct {
	oldOf func(roleID int64) int
	newOf func(roleID int64) int
}

func newRouter(strategy string, old, newCount, replicas int) router {
	if strategy == "modulus" {
		return router{
			oldOf: func(id int64) int { return int(shard.ForRole(id, int32(old))) },
			newOf: func(id int64) int { return int(shard.ForRole(id, int32(newCount))) },
		}
	}
	// ring：用节点名 game-shard-i 建环，返回其序号。
	oldRing := shard.RingForShards(int32(old), replicas)
	newRing := shard.RingForShards(int32(newCount), replicas)
	return router{
		oldOf: func(id int64) int { return nodeIndex(oldRing.Get(id)) },
		newOf: func(id int64) int { return nodeIndex(newRing.Get(id)) },
	}
}

// nodeIndex 从 game-shard-N 提取 N。
func nodeIndex(name string) int {
	if i := strings.LastIndex(name, "-"); i >= 0 {
		n := 0
		_, err := fmt.Sscanf(name[i+1:], "%d", &n)
		if err == nil {
			return n
		}
	}
	return -1
}

// runPlan 规划模式：扫描 id 区间，统计迁移比例与 from→to 分布。
func runPlan(r router, old, newCount int, strategy string, start, end int64) {
	if end <= start {
		fmt.Println("range-end must be > range-start")
		return
	}
	total := end - start
	var moved int64
	moves := make(map[[2]int]int64) // [from,to] -> count
	for id := start; id < end; id++ {
		o, n := r.oldOf(id), r.newOf(id)
		if o != n {
			moved++
			moves[[2]int{o, n}]++
		}
	}
	frac := float64(moved) / float64(total)
	fmt.Printf("== reshard plan (%s) %d → %d 分片 ==\n", strategy, old, newCount)
	fmt.Printf("scanned role_id: [%d, %d)  total=%d\n", start, end, total)
	fmt.Printf("need migrate:    %d (%.2f%%)\n", moved, frac*100)
	fmt.Printf("distinct routes: %d\n", len(moves))
	// 取模会几乎全量迁移，提示用 ring。
	if strategy == "modulus" && frac > 0.5 {
		fmt.Println("提示: 取模迁移量过大，建议改用 -strategy ring 降低迁移成本。")
	}
}

// runExecute 执行模式：在旧/新 PG 分库布局间迁移变动的 roles 行。
func runExecute(pgOld, pgNew string, r router, batch int, deleteSource bool) error {
	oldDSNs := splitCSV(pgOld)
	newDSNs := splitCSV(pgNew)
	if len(oldDSNs) == 0 || len(newDSNs) == 0 {
		return fmt.Errorf("execute 模式需 -pg-old 与 -pg-new")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	oldPool, err := db.NewShardedPool(ctx, oldDSNs)
	if err != nil {
		return fmt.Errorf("connect old: %w", err)
	}
	defer oldPool.Close()
	newPool, err := db.NewShardedPool(ctx, newDSNs)
	if err != nil {
		return fmt.Errorf("connect new: %w", err)
	}
	defer newPool.Close()

	var scanned, migrated int64
	for shardIdx, pool := range oldPool.All() {
		rows, err := pool.Query(ctx,
			`SELECT id, account_id, zone_id, name, level, snapshot, version, owner_epoch,
			        created_at, updated_at FROM roles`)
		if err != nil {
			return fmt.Errorf("scan old shard %d: %w", shardIdx, err)
		}
		type roleRow struct {
			id         int64
			accountID  int64
			zoneID     int32
			name       string
			level      int32
			snap       []byte
			version    int64
			ownerEpoch int64
			createdAt  time.Time
			updatedAt  time.Time
		}
		var toMove []roleRow
		for rows.Next() {
			var rr roleRow
			if err := rows.Scan(
				&rr.id, &rr.accountID, &rr.zoneID, &rr.name, &rr.level, &rr.snap,
				&rr.version, &rr.ownerEpoch, &rr.createdAt, &rr.updatedAt,
			); err != nil {
				rows.Close()
				return err
			}
			scanned++
			// 仅迁移物理库发生变化的角色。
			if r.oldOf(rr.id)%len(oldDSNs) != r.newOf(rr.id)%len(newDSNs) {
				toMove = append(toMove, rr)
			}
		}
		rows.Close()

		for i := 0; i < len(toMove); i += batch {
			end := i + batch
			if end > len(toMove) {
				end = len(toMove)
			}
			for _, rr := range toMove[i:end] {
				dstIndex := r.newOf(rr.id) % len(newDSNs)
				srcIndex := r.oldOf(rr.id) % len(oldDSNs)
				if oldDSNs[srcIndex] == newDSNs[dstIndex] {
					continue
				}
				dst := newPool.All()[dstIndex]
				_, err := dst.Exec(ctx,
					`INSERT INTO roles
					    (id, account_id, zone_id, name, level, snapshot, version, owner_epoch, created_at, updated_at)
					 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
					 ON CONFLICT (id) DO UPDATE
					   SET account_id=EXCLUDED.account_id, zone_id=EXCLUDED.zone_id,
					       name=EXCLUDED.name, level=EXCLUDED.level, snapshot=EXCLUDED.snapshot,
					       version=EXCLUDED.version, owner_epoch=EXCLUDED.owner_epoch,
					       updated_at=EXCLUDED.updated_at`,
					rr.id, rr.accountID, rr.zoneID, rr.name, rr.level, rr.snap,
					rr.version, rr.ownerEpoch+1, rr.createdAt, rr.updatedAt)
				if err != nil {
					return fmt.Errorf("insert role %d: %w", rr.id, err)
				}
				var copiedVersion, copiedEpoch int64
				var copiedSnap []byte
				if err := dst.QueryRow(ctx,
					`SELECT version, owner_epoch, snapshot FROM roles WHERE id=$1`, rr.id,
				).Scan(&copiedVersion, &copiedEpoch, &copiedSnap); err != nil {
					return fmt.Errorf("verify role %d: %w", rr.id, err)
				}
				if copiedVersion != rr.version || copiedEpoch != rr.ownerEpoch+1 || string(copiedSnap) != string(rr.snap) {
					return fmt.Errorf("verify role %d: destination differs", rr.id)
				}
				if deleteSource {
					tag, err := pool.Exec(ctx,
						`DELETE FROM roles WHERE id=$1 AND version=$2 AND owner_epoch=$3`,
						rr.id, rr.version, rr.ownerEpoch)
					if err != nil {
						return fmt.Errorf("delete role %d: %w", rr.id, err)
					}
					if tag.RowsAffected() != 1 {
						return fmt.Errorf("delete role %d: source changed during copy", rr.id)
					}
				}
				migrated++
			}
			fmt.Printf("shard %d: migrated %d/%d\n", shardIdx, end, len(toMove))
		}
	}
	fmt.Printf("done: scanned=%d migrated=%d\n", scanned, migrated)
	return nil
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
