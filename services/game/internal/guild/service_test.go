package guild_test

import (
	"context"
	"testing"

	"newgame/services/game/internal/guild"
)

func TestJoinGuild(t *testing.T) {
	s := guild.New(nil, 1)
	g, err := s.Join(context.Background(), 10001, 1)
	if err != nil {
		t.Fatal(err)
	}
	if g.ID != 1 {
		t.Fatalf("guild id %d", g.ID)
	}
}
