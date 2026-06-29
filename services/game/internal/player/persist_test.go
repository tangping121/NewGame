package player_test

import (
	"testing"

	"newgame/pkg/protocol"
	"newgame/pkg/repo"
	"newgame/services/game/internal/player"
)

func TestMutatingAct(t *testing.T) {
	cases := []struct {
		act    uint16
		mutate bool
		name   string
	}{
		{protocol.ActPlayerData, false, "player data"},
		{protocol.ActSkillList, false, "skill list"},
		{protocol.ActSkillUpgrade, true, "skill upgrade"},
		{protocol.ActQuestList, false, "quest list"},
		{protocol.ActQuestAccept, true, "quest accept"},
	}
	for _, c := range cases {
		got := player.MutatingAct(protocol.CmdGame, c.act)
		if got != c.mutate {
			t.Fatalf("%s: mutate=%v want %v", c.name, got, c.mutate)
		}
	}
	if player.MutatingAct(protocol.CmdLogin, protocol.ActLogin) {
		t.Fatal("non-game cmd should not mutate")
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
