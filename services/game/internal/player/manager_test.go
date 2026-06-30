package player_test

import (
	"context"
	"sync"
	"testing"

	"newgame/services/game/internal/player"
)

// TestGetConcurrentSingleActor 并发首次 Get 同一 roleID 应只产生一个 Actor 实例。
func TestGetConcurrentSingleActor(t *testing.T) {
	m := player.NewManager(nil, player.PersistConfig{})
	const (
		roleID = int64(42)
		n      = 32
	)
	var wg sync.WaitGroup
	actors := make([]*player.Actor, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			actors[idx] = m.Get(context.Background(), roleID)
		}(i)
	}
	wg.Wait()
	first := actors[0]
	if first == nil {
		t.Fatal("nil actor")
	}
	for i := 1; i < n; i++ {
		if actors[i] != first {
			t.Fatalf("actor %d pointer %p != first %p", i, actors[i], first)
		}
	}
	if m.Online() != 1 {
		t.Fatalf("online = %d want 1", m.Online())
	}
}
