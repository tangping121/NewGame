// Package shard 提供角色到 Game 分片的确定性路由，支撑 10 万～100 万在线水平扩展。
package shard

import (
	"fmt"
	"hash/fnv"
)

// DefaultShardCount 单区推荐默认分片数（10 万 CCU 量级：每片约 2000 人）。
const DefaultShardCount int32 = 50

// MaxRecommendedShardCount 单区上限规划（100 万 CCU：每片约 2000 人）。
const MaxRecommendedShardCount int32 = 512

// CCUPerShard 每 Game 分片建议承载的在线人数（含峰值缓冲）。
const CCUPerShard int32 = 2000

// ForRole 按 role_id 取模路由到分片 [0, shardCount)。
//
// 参数:
//   - roleID: 角色 ID，全局唯一
//   - shardCount: 分片总数，必须 > 0
//
// 返回: 分片编号 shard_id
func ForRole(roleID int64, shardCount int32) int32 {
	if shardCount <= 0 {
		shardCount = 1
	}
	mod := roleID % int64(shardCount)
	if mod < 0 {
		mod += int64(shardCount)
	}
	return int32(mod)
}

// ForRoleFNV 使用 FNV 哈希路由，适合 role_id 分布不均或需与外部系统对齐的场景。
func ForRoleFNV(roleID int64, shardCount int32) int32 {
	if shardCount <= 0 {
		shardCount = 1
	}
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%d", roleID)
	return int32(h.Sum32() % uint32(shardCount))
}

// ShardCountForCCU 根据目标在线人数估算所需 Game 分片数（向上取整）。
//
// 参数:
//   - ccu: 目标同时在线人数
//   - perShard: 每片承载人数；<=0 时使用 CCUPerShard
func ShardCountForCCU(ccu int32, perShard int32) int32 {
	if perShard <= 0 {
		perShard = CCUPerShard
	}
	if ccu <= 0 {
		return 1
	}
	n := (ccu + perShard - 1) / perShard
	if n > MaxRecommendedShardCount {
		return MaxRecommendedShardCount
	}
	if n < 1 {
		return 1
	}
	return n
}

// ServiceName 生成分片 Game 服务发现名，如 game-shard-3。
func ServiceName(shardID int32) string {
	return fmt.Sprintf("game-shard-%d", shardID)
}

// GateServiceName 生成 Gate 实例发现名前缀，如 gate-2（配合实例 ID 使用）。
func GateServiceName(instanceSuffix string) string {
	return "gate-" + instanceSuffix
}
