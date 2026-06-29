// Package redis 封装 go-redis 客户端与连通性检查，支持单机与 Cluster。
package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// Client 是项目内统一使用的 Redis 客户端类型。
//
// 采用 UniversalClient 接口，使单机（*redis.Client）与集群（*redis.ClusterClient）
// 对上层调用透明，支撑 presence/排行榜等热 key 在 100 万 CCU 时拆分到 Cluster。
type Client = goredis.UniversalClient

// NewClient 创建单机 Redis 客户端。
func NewClient(addr string) Client {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: []string{addr},
	})
}

// NewCluster 创建 Redis Cluster 客户端。
//
// 参数:
//   - addrs: 集群任意若干节点地址；go-redis 会自动发现拓扑
func NewCluster(addrs []string) Client {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: addrs,
	})
}

// New 根据配置选择单机或集群：addrs 非空走 Cluster，否则用单机 addr。
func New(addr string, clusterAddrs []string) Client {
	if len(clusterAddrs) > 0 {
		return NewCluster(clusterAddrs)
	}
	return NewClient(addr)
}

// Ping 检查连通性。
func Ping(ctx context.Context, c Client) error {
	return c.Ping(ctx).Err()
}
