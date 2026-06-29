package player

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"newgame/pkg/repo"
)

// PersistConfig 玩家状态持久化策略（P2：异步落库支撑 10 万 CCU）。
type PersistConfig struct {
	Mode        string        // async | sync；空则 sync
	Interval    time.Duration // 异步 flush 间隔
	Concurrency int           // 异步 flush 并发数；<=0 默认 16
}

// AsyncSaver 后台批量将脏 Actor 写入 Postgres，避免每帧同步 UPDATE。
type AsyncSaver struct {
	roles       *repo.RoleRepo
	interval    time.Duration
	concurrency int
	mu          sync.Mutex
	pending     map[int64]*Actor
	depth       atomic.Int64
	saved       atomic.Int64 // 累计成功落库次数
	failed      atomic.Int64 // 累计失败次数（已重入队）
}

// NewAsyncSaver 创建异步落库器；roles 为 nil 时不写库。
//
// 参数:
//   - roles: 角色仓库
//   - interval: flush 间隔；<=0 默认 10s
//   - concurrency: 单次 flush 的并发落库数；<=0 默认 16
func NewAsyncSaver(roles *repo.RoleRepo, interval time.Duration, concurrency int) *AsyncSaver {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	return &AsyncSaver{
		roles:       roles,
		interval:    interval,
		concurrency: concurrency,
		pending:     make(map[int64]*Actor),
	}
}

// Stats 返回累计成功/失败落库次数（监控用）。
func (s *AsyncSaver) Stats() (saved, failed int64) {
	if s == nil {
		return 0, 0
	}
	return s.saved.Load(), s.failed.Load()
}

// Schedule 标记玩家待落库（合并多次修改为一次 flush）。
func (s *AsyncSaver) Schedule(a *Actor) {
	if s == nil || s.roles == nil || a == nil {
		return
	}
	s.mu.Lock()
	s.pending[a.ID] = a
	n := int64(len(s.pending))
	s.mu.Unlock()
	s.depth.Store(n)
}

// QueueDepth 当前待落库玩家数（监控指标 ng_game_save_queue_depth）。
func (s *AsyncSaver) QueueDepth() int64 {
	if s == nil {
		return 0
	}
	return s.depth.Load()
}

// Run 启动定时 flush；ctx 取消时执行最后一次全量 flush。
func (s *AsyncSaver) Run(ctx context.Context) {
	if s == nil || s.roles == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flushAll(context.Background())
			return
		case <-ticker.C:
			s.flushAll(ctx)
		}
	}
}

// flushAll 并发落库当前批次；失败的 Actor 重新入队，下次再试。
func (s *AsyncSaver) flushAll(ctx context.Context) {
	s.mu.Lock()
	batch := s.pending
	s.pending = make(map[int64]*Actor)
	s.depth.Store(0)
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	jobs := make(chan *Actor, len(batch))
	for _, a := range batch {
		jobs <- a
	}
	close(jobs)

	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range jobs {
				if err := a.Save(ctx); err != nil {
					s.failed.Add(1)
					s.Schedule(a) // 失败重入队，下个周期重试
					continue
				}
				s.saved.Add(1)
			}
		}()
	}
	wg.Wait()
}

// FlushPending 立即落库当前所有待写 Actor（进程关停时调用，防丢数据）。
func (s *AsyncSaver) FlushPending(ctx context.Context) {
	if s == nil {
		return
	}
	s.flushAll(ctx)
}

// FlushNow 立即同步落库（支付、拍卖等关键路径）。
func (s *AsyncSaver) FlushNow(ctx context.Context, a *Actor) error {
	if a == nil {
		return nil
	}
	if s != nil {
		s.mu.Lock()
		delete(s.pending, a.ID)
		s.depth.Store(int64(len(s.pending)))
		s.mu.Unlock()
	}
	return a.Save(ctx)
}
