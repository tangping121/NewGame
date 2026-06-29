package db

import (
	"context"
	"testing"
)

func TestNewShardedPoolEmpty(t *testing.T) {
	if _, err := NewShardedPool(context.Background(), nil); err == nil {
		t.Fatal("expected error for empty dsn list")
	}
}

// TestShardedPoolForRoleStable 验证同一 role_id 始终路由到同一分库索引。
// 使用 nil 占位池（ForRole 只做索引选择，不解引用），避免依赖真实数据库。
func TestShardedPoolForRoleStable(t *testing.T) {
	sp := &ShardedPool{pools: make([]*poolHandle, 4)}
	if sp.Count() != 4 {
		t.Fatalf("count %d", sp.Count())
	}
	for _, roleID := range []int64{1, 2, 100, 99999, 123456789} {
		a := sp.indexForRole(roleID)
		b := sp.indexForRole(roleID)
		if a != b || a < 0 || a >= 4 {
			t.Fatalf("role %d unstable/out-of-range index %d vs %d", roleID, a, b)
		}
	}
}
