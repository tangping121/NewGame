package discovery

import (
	"context"
	"sync"
	"sync/atomic"
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

func TestResolverSingleflight(t *testing.T) {
	r := NewResolver(nil, time.Second)
	var calls atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			inst, ok := r.resolve(context.Background(), "game:1", func() (Instance, error) {
				calls.Add(1)
				time.Sleep(30 * time.Millisecond)
				return Instance{ID: "shard-a"}, nil
			})
			if !ok || inst.ID != "shard-a" {
				t.Errorf("resolve = %+v ok=%v", inst, ok)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent refresh called pick %d times", calls.Load())
	}
	_, _ = r.resolve(context.Background(), "game:1", func() (Instance, error) {
		calls.Add(1)
		return Instance{ID: "other"}, nil
	})
	if calls.Load() != 1 {
		t.Fatalf("ttl hit should skip pick, calls=%d", calls.Load())
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
