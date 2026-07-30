package session

import "testing"

func TestValidTokenBounds(t *testing.T) {
	if validToken("short") {
		t.Fatal("short token accepted")
	}
	if !validToken("tk_0123456789abcdef0123456789abcdef") {
		t.Fatal("generated token shape rejected")
	}
	if validToken(string(make([]byte, 129))) {
		t.Fatal("oversized token accepted")
	}
}
