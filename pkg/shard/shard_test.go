package shard_test

import (
	"testing"

	"newgame/pkg/shard"
)

func TestForRole(t *testing.T) {
	if got := shard.ForRole(10001, 50); got < 0 || got >= 50 {
		t.Fatalf("shard %d", got)
	}
	// 同一 role 始终落同一分片
	if a, b := shard.ForRole(99999, 50), shard.ForRole(99999, 50); a != b {
		t.Fatalf("not stable %d vs %d", a, b)
	}
}

func TestShardCountForCCU(t *testing.T) {
	if n := shard.ShardCountForCCU(100_000, 2000); n != 50 {
		t.Fatalf("100k need 50 shards, got %d", n)
	}
	if n := shard.ShardCountForCCU(1_000_000, 2000); n != 500 {
		t.Fatalf("1m need 500 shards, got %d", n)
	}
}
