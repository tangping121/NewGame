// Package actor 提供玩家 Actor 模型的消息邮箱，保证单玩家逻辑串行执行。
package actor

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed 邮箱已关闭（玩家已下线/淘汰）时投递返回。
var ErrClosed = errors.New("mailbox closed")

// Mailbox 为单个玩家串行投递闭包任务，避免并发写玩家状态。
type Mailbox struct {
	ch     chan func()  // 待执行任务队列
	once   sync.Once    // 保证 loop 只启动一次
	mu     sync.RWMutex // 保护 closed，避免向已关闭 channel 投递
	closed bool         // 是否已关闭
}

// NewMailbox 创建邮箱并启动后台消费 goroutine。
//
// 参数:
//   - size: 队列缓冲长度；<=0 时默认 256
func NewMailbox(size int) *Mailbox {
	if size <= 0 {
		size = 256
	}
	m := &Mailbox{ch: make(chan func(), size)}
	m.once.Do(func() {
		go m.loop()
	})
	return m
}

func (m *Mailbox) loop() {
	for fn := range m.ch {
		if fn != nil {
			fn()
		}
	}
}

// Post 异步投递任务到该玩家邮箱；邮箱已关闭时返回 ErrClosed。
//
// 参数:
//   - fn: 在玩家串行上下文中执行的函数
func (m *Mailbox) Post(fn func()) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	m.ch <- fn
	return nil
}

// Call 同步投递任务并等待结果；ctx 取消时不再等待执行完成。
//
// 持有读锁完成投递，确保与 Close（写锁）互斥，杜绝向已关闭 channel 发送。
func (m *Mailbox) Call(ctx context.Context, fn func() ([]byte, error)) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)

	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, ErrClosed
	}
	select {
	case m.ch <- func() {
		d, e := fn()
		ch <- result{d, e}
	}:
		m.mu.RUnlock()
	case <-ctx.Done():
		m.mu.RUnlock()
		return nil, ctx.Err()
	}

	select {
	case r := <-ch:
		return r.data, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close 关闭邮箱，不再接受新任务（幂等）。已入队任务仍会被执行完。
func (m *Mailbox) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.ch)
}

// Player 游戏 Actor 需实现的接口（Gate/Game 消息处理）。
type Player interface {
	PlayerID() int64
	Handle(ctx context.Context, cmd, act uint16, payload []byte) ([]byte, error)
	Save(ctx context.Context) error
}
