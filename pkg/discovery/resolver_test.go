package discovery

import (
	"context"
	"testing"
	"time"
)

func TestResolverNilSafe(t *testing.T) {
	var r *Resolver
	if _, ok := r.Resolve(context.Background(), "game", 1); ok {
		t.Fatal("nil resolver should return not-ok")
	}
	r2 := NewResolver(nil, 0)
	if _, ok := r2.Resolve(context.Background(), "game", 1); ok {
		t.Fatal("nil registry should return not-ok")
	}
}

func TestResolverDefaultTTL(t *testing.T) {
	r := NewResolver(nil, 0)
	if r.ttl != 2*time.Second {
		t.Fatalf("default ttl %v", r.ttl)
	}
}

func TestItoa(t *testing.T) {
	cases := map[int32]string{0: "0", 1: "1", 42: "42", -7: "-7", 100: "100"}
	for in, want := range cases {
		if got := itoa(in); got != want {
			t.Fatalf("itoa(%d)=%s want %s", in, got, want)
		}
	}
}
