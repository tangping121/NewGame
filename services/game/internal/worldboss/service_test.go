package worldboss_test

import (
	"context"
	"testing"

	"newgame/services/game/internal/worldboss"
)

func TestAttackWithoutRedis(t *testing.T) {
	s := worldboss.New(nil)
	st, err := s.Attack(context.Background(), 10001, 1, 500)
	if err != nil {
		t.Fatal(err)
	}
	if st.HP != 0 {
		t.Fatalf("expected 0 hp without redis, got %d", st.HP)
	}
}
