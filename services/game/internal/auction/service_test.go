package auction_test

import (
	"context"
	"testing"

	"newgame/services/game/internal/auction"
	"newgame/services/game/internal/player"
)

func TestAuctionMemory(t *testing.T) {
	mgr := player.NewManager(nil, player.PersistConfig{})
	s := auction.New(nil, mgr)
	ctx := context.Background()

	seller := mgr.Get(ctx, 10001)
	seller.Inv.Add("potion", 5)
	seller.Gold = 0

	l, err := s.Create(ctx, 10001, "potion", 2, 100)
	if err != nil {
		t.Fatal(err)
	}

	buyer := mgr.Get(ctx, 10002)
	buyer.Gold = 200
	got, err := s.Buy(ctx, 10002, l.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ItemID != "potion" || got.Qty != 2 {
		t.Fatalf("listing %+v", got)
	}
	if buyer.Gold != 100 {
		t.Fatalf("buyer gold %d", buyer.Gold)
	}
	if seller.Gold != 100 {
		t.Fatalf("seller gold %d", seller.Gold)
	}
	if buyer.Inv["potion"] != 2 {
		t.Fatalf("buyer potion %d", buyer.Inv["potion"])
	}
}
