package rankkey

import "testing"

func TestKeys(t *testing.T) {
	if Zone(1, "dungeon") != "rank:zone:1:dungeon" {
		t.Fatal("zone key mismatch")
	}
	if Global("dungeon") != "rank:global:dungeon" {
		t.Fatal("global key mismatch")
	}
	if Member(2, 20002) != "z2:20002" {
		t.Fatal("member mismatch")
	}
}
