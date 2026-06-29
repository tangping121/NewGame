package shard_test

import (
	"fmt"
	"testing"

	"newgame/pkg/shard"
)

func TestRingStableMapping(t *testing.T) {
	r := shard.RingForShards(50, 100)
	if r.Nodes() != 50 {
		t.Fatalf("nodes %d", r.Nodes())
	}
	// 同一 role 多次查询稳定
	for _, id := range []int64{1, 2, 100, 99999, 123456789} {
		if r.Get(id) != r.Get(id) {
			t.Fatalf("unstable mapping for %d", id)
		}
		if r.Get(id) == "" {
			t.Fatalf("empty node for %d", id)
		}
	}
}

// TestConsistentHashMovesFewerKeys 验证扩容时一致性哈希迁移量远小于取模。
func TestConsistentHashMovesFewerKeys(t *testing.T) {
	const n = 50000
	// 取模：50 -> 51
	modMoved := 0
	for id := int64(0); id < n; id++ {
		if shard.ForRole(id, 50) != shard.ForRole(id, 51) {
			modMoved++
		}
	}

	// 一致性哈希：50 -> 51 个节点
	r50 := shard.RingForShards(50, 200)
	r51 := shard.RingForShards(51, 200)
	ringMoved := 0
	for id := int64(0); id < n; id++ {
		if r50.Get(id) != r51.Get(id) {
			ringMoved++
		}
	}

	modFrac := float64(modMoved) / n
	ringFrac := float64(ringMoved) / n
	fmt.Printf("rescale 50->51 moved: modulus=%.1f%%  ring=%.1f%%\n", modFrac*100, ringFrac*100)

	// 取模应几乎全量迁移；一致性哈希应远低于取模（通常 < 10%）。
	if modFrac < 0.8 {
		t.Fatalf("expected modulus to move most keys, got %.2f", modFrac)
	}
	if ringFrac > modFrac/2 {
		t.Fatalf("consistent hash moved too many keys: %.2f vs modulus %.2f", ringFrac, modFrac)
	}
}
