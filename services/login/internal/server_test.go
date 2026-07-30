package internal

import "testing"

func TestLoginRateKeyNormalizesUsername(t *testing.T) {
	first := loginRateKey(" PlayerOne ")
	second := loginRateKey("playerone")
	if first != second {
		t.Fatalf("normalized usernames produced different keys: %q != %q", first, second)
	}
	if first == loginRateKey("player-two") {
		t.Fatal("different usernames produced the same key")
	}
}
