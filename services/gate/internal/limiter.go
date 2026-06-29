package internal

import (
	"net"
	"sync"
)

// connLimiter 按客户端 IP 限制 Gate 同时建立的 TCP 连接数，防刷连接。
type connLimiter struct {
	mu    sync.Mutex
	count map[string]int // IP -> 当前连接数
	max   int            // 单 IP 上限
}

// newConnLimiter 创建限流器。
//
// 参数:
//   - max: 单 IP 最大连接数；<=0 时默认 32
func newConnLimiter(max int) *connLimiter {
	if max <= 0 {
		max = 32
	}
	return &connLimiter{count: make(map[string]int), max: max}
}

// acquire 新连接建立时调用；超过上限返回 false，调用方应 close 连接。
//
// 参数:
//   - addr: net.Conn.RemoteAddr()
func (l *connLimiter) acquire(addr net.Addr) bool {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count[host] >= l.max {
		return false
	}
	l.count[host]++
	return true
}

// release 连接关闭时调用，递减计数。
func (l *connLimiter) release(addr net.Addr) {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count[host] > 0 {
		l.count[host]--
	}
	if l.count[host] == 0 {
		delete(l.count, host)
	}
}
