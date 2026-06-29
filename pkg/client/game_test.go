package client

import (
	"context"
	"testing"
)

// TestGameClientFallbackByZone 无服务发现时按区服回退到本地端口。
func TestGameClientFallbackByZone(t *testing.T) {
	c1 := NewGameClientSharded(nil, 1, 50)
	if got := c1.baseURLForRole(context.Background(), 10001); got != "http://127.0.0.1:9100" {
		t.Fatalf("zone1 fallback %s", got)
	}
	c2 := NewGameClientSharded(nil, 2, 50)
	if got := c2.baseURLForRole(context.Background(), 10001); got != "http://127.0.0.1:9110" {
		t.Fatalf("zone2 fallback %s", got)
	}
}

// TestGameClientSingleProcess shardCount<=0 时退化为单进程模式。
func TestGameClientSingleProcess(t *testing.T) {
	c := NewGameClient(nil, 1)
	if c.shardCount != 0 {
		t.Fatalf("expected single-process shardCount=0, got %d", c.shardCount)
	}
}
