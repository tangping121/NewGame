package config

import (
	"os"
	"testing"
)

func TestApplyEnvOverrides(t *testing.T) {
	t.Setenv("NG_SHARD_COUNT", "64")
	t.Setenv("NG_SHARD_ID", "7")
	t.Setenv("NG_ZONE_ID", "2")
	t.Setenv("NG_REDIS_CLUSTER", "a:6379, b:6379 ,c:6379")
	t.Setenv("NG_POSTGRES_SHARDS", "dsn0,dsn1")

	var svc Service
	applyEnvOverrides(&svc)

	if svc.Scale.ShardCount != 64 || svc.Scale.ShardID != 7 {
		t.Fatalf("shard %d/%d", svc.Scale.ShardID, svc.Scale.ShardCount)
	}
	if svc.ZoneID != 2 {
		t.Fatalf("zone %d", svc.ZoneID)
	}
	if len(svc.Infra.RedisCluster) != 3 || svc.Infra.RedisCluster[1] != "b:6379" {
		t.Fatalf("redis cluster %#v", svc.Infra.RedisCluster)
	}
	if len(svc.Infra.PostgresShards) != 2 {
		t.Fatalf("pg shards %#v", svc.Infra.PostgresShards)
	}
}

func TestValidate(t *testing.T) {
	// 合法：分片号在范围内
	ok := Service{Scale: Scale{ShardCount: 50, ShardID: 3, SaveMode: "async"}, Gate: GateScale{GameTransport: "grpc"}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
	// 非法：shard_id 越界
	bad := Service{Scale: Scale{ShardCount: 50, ShardID: 50}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected shard_id out of range error")
	}
	// 非法：save_mode
	badMode := Service{Scale: Scale{SaveMode: "weird"}}
	if err := badMode.Validate(); err == nil {
		t.Fatal("expected save_mode error")
	}
	// 非法：transport
	badTrans := Service{Gate: GateScale{GameTransport: "udp"}}
	if err := badTrans.Validate(); err == nil {
		t.Fatal("expected transport error")
	}
	// 合法：单进程（shard_count=0）
	zero := Service{}
	if err := zero.Validate(); err != nil {
		t.Fatalf("zero config should be valid, got %v", err)
	}
}

func TestShardIDFromPodName(t *testing.T) {
	os.Unsetenv("NG_SHARD_ID")
	t.Setenv("POD_NAME", "game-shard-12")
	id, ok := shardIDFromEnv()
	if !ok || id != 12 {
		t.Fatalf("pod name shard %d ok=%v", id, ok)
	}
}
