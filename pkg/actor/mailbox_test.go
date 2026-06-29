package actor_test

import (
	"context"
	"testing"
	"time"

	"newgame/pkg/actor"
)

func TestMailboxCallSerial(t *testing.T) {
	m := actor.NewMailbox(8)
	var n int
	for i := 0; i < 20; i++ {
		_, err := m.Call(context.Background(), func() ([]byte, error) {
			n++
			return []byte("ok"), nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if n != 20 {
		t.Fatalf("expected 20 serial calls, got %d", n)
	}
}

func TestMailboxClosedRejects(t *testing.T) {
	m := actor.NewMailbox(4)
	m.Close()
	if err := m.Post(func() {}); err != actor.ErrClosed {
		t.Fatalf("expected ErrClosed from Post, got %v", err)
	}
	if _, err := m.Call(context.Background(), func() ([]byte, error) { return nil, nil }); err != actor.ErrClosed {
		t.Fatalf("expected ErrClosed from Call, got %v", err)
	}
	// 重复 Close 应幂等不 panic
	m.Close()
}

func TestMailboxCallContextCancel(t *testing.T) {
	m := actor.NewMailbox(1)
	m.Post(func() { time.Sleep(200 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := m.Call(ctx, func() ([]byte, error) {
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected context error")
	}
}
