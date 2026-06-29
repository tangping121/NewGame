package db

import (
	"context"
	"fmt"

	"newgame/pkg/shard"

	"github.com/jackc/pgx/v5/pgxpool"
)

// poolHandle 是 pgxpool.Pool 的别名，便于测试占位。
type poolHandle = pgxpool.Pool

// ShardedPool 按 role_id 将玩家数据路由到多个 Postgres 实例（分库）。
//
// 用途：单库写入达到瓶颈时（~50 万 CCU 起），按 role_id 哈希拆分到 N 个库，
// 每个库独立连接池。分库数与 Game 分片数可不同，但建议成倍数关系便于运维。
type ShardedPool struct {
	pools []*poolHandle
}

// NewShardedPool 为每个 DSN 建立连接池。
//
// 参数:
//   - ctx: 建池超时上下文
//   - dsns: 各分库 DSN，顺序即分库编号 0..len-1，不可为空
//
// 返回: 任一 DSN 建池失败时回滚已建池并返回 error
func NewShardedPool(ctx context.Context, dsns []string) (*ShardedPool, error) {
	if len(dsns) == 0 {
		return nil, fmt.Errorf("no postgres shards configured")
	}
	sp := &ShardedPool{pools: make([]*poolHandle, 0, len(dsns))}
	for i, dsn := range dsns {
		p, err := NewPool(ctx, dsn)
		if err != nil {
			sp.Close()
			return nil, fmt.Errorf("postgres shard %d: %w", i, err)
		}
		sp.pools = append(sp.pools, p)
	}
	return sp, nil
}

// Count 返回分库数量。
func (s *ShardedPool) Count() int {
	return len(s.pools)
}

// indexForRole 计算 role_id 对应的分库索引（确定性哈希，与 shard 包一致）。
func (s *ShardedPool) indexForRole(roleID int64) int {
	if len(s.pools) <= 1 {
		return 0
	}
	return int(shard.ForRole(roleID, int32(len(s.pools))))
}

// ForRole 按 role_id 路由到对应分库连接池。
func (s *ShardedPool) ForRole(roleID int64) *pgxpool.Pool {
	return s.pools[s.indexForRole(roleID)]
}

// All 返回全部分库连接池，用于跨库聚合（如全服统计、迁移）。
func (s *ShardedPool) All() []*pgxpool.Pool {
	return s.pools
}

// Close 关闭所有连接池。
func (s *ShardedPool) Close() {
	for _, p := range s.pools {
		if p != nil {
			p.Close()
		}
	}
	s.pools = nil
}
