package grant_test

import (
	"testing"

	"newgame/pkg/grant"
)

func TestParse(t *testing.T) {
	b, err := grant.Parse("gold:100,potion:2")
	if err != nil {
		t.Fatal(err)
	}
	if b.Gold != 100 || b.Items["potion"] != 2 {
		t.Fatalf("unexpected bundle: %+v", b)
	}
}

func TestParseEmpty(t *testing.T) {
	b, err := grant.Parse("")
	if err != nil {
		t.Fatal(err)
	}
	if b.Gold != 0 || len(b.Items) != 0 {
		t.Fatalf("expected empty bundle")
	}
}
