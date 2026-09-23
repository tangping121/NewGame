package auth_test

import (
	"testing"

	"bastion/pkg/auth"
)

func TestPasswordHash(t *testing.T) {
	hash, err := auth.HashPassword("secret")
	if err != nil {
		t.Fatal(err)
	}
	if !auth.CheckPassword(hash, "secret") {
		t.Fatal("expected password match")
	}
	if auth.CheckPassword(hash, "wrong") {
		t.Fatal("expected password mismatch")
	}
}
