package guildwar_test

import (
	"context"
	"testing"

	"newgame/services/game/internal/guildwar"
)

func TestAttackMemory(t *testing.T) {
	s := guildwar.New(nil)
	st, err := s.Attack(context.Background(), 1, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Top) != 1 || st.Top[0].Score != 500 {
		t.Fatalf("top %+v", st.Top)
	}
}

func TestResetSeasonMemory(t *testing.T) {
	s := guildwar.New(nil)
	_, _ = s.Attack(context.Background(), 1, 100)
	st, err := s.ResetSeason(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Season != 2 {
		t.Fatalf("season %d", st.Season)
	}
	st2, _ := s.State(context.Background())
	if len(st2.Top) != 0 {
		t.Fatalf("expected empty top after reset, got %+v", st2.Top)
	}
}
