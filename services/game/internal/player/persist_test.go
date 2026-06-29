package player_test

import (
	"testing"

	"newgame/pkg/repo"
	"newgame/services/game/internal/player"
)

func TestMutatingAct(t *testing.T) {
	if !player.MutatingAct(2, 4) {
		t.Fatal("skill upgrade should mutate")
	}
	if player.MutatingAct(2, 1) {
		t.Fatal("player data read should not mutate")
	}
}

func TestAsyncSaverNilRolesNoOp(t *testing.T) {
	s := player.NewAsyncSaver(nil, 0, 0)
	a := player.New(1, repo.RoleSnapshot{Level: 1}, nil)
	s.Schedule(a)
	if s.QueueDepth() != 0 {
		t.Fatalf("nil repo should not queue, depth %d", s.QueueDepth())
	}
}
