package internal_test

import (
	"testing"

	"newgame/services/match/internal"
)

func TestParseMember(t *testing.T) {
	e, err := internal.ParseMember("z2:10002")
	if err != nil {
		t.Fatal(err)
	}
	if e.ZoneID != 2 || e.RoleID != 10002 {
		t.Fatalf("entry %+v", e)
	}
	key := internal.MemberKey(1, 10001)
	if key != "z1:10001" {
		t.Fatalf("key %s", key)
	}
}
