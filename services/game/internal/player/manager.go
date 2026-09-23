package player

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"bastion/pkg/grant"
	"bastion/pkg/protocol"
	"bastion/pkg/repo"
	"bastion/pkg/shard"
)

// idleTimeout 玩家无活动超过此时长后被淘汰（落库 + 关闭邮箱）。
const idleTimeout = 30 * time.Minute

var ErrWrongShard = errors.New("role belongs to another shard")

// Manager 在线玩家 Actor 缓存：同一 roleID 复用同一 Actor 实例。
type Manager struct {
	roles      *repo.RoleRepo // 角色持久化；nil 时使用默认空快照
	players    sync.Map       // roleID -> *Actor
	saver      *AsyncSaver    // 异步落库；nil 时 ScheduleSave 退化为同步 Save
	loadLocks  [256]sync.Mutex
	shardID    int32
	shardCount int32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewManager 创建玩家管理器。
//
// 参数:
//   - roles: Postgres 角色仓库；可为 nil（纯内存模式）
//   - persist: 落库策略；Mode=async 时启动后台 flush
func NewManager(roles *repo.RoleRepo, persist PersistConfig) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		roles:      roles,
		shardID:    persist.ShardID,
		shardCount: persist.ShardCount,
		ctx:        ctx,
		cancel:     cancel,
	}
	if roles != nil && persist.Mode == "async" {
		m.saver = NewAsyncSaver(roles, persist.Interval, persist.Concurrency)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.saver.Run(ctx)
		}()
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.evictLoop(ctx)
	}()
	return m
}

// Saver 返回异步落库器（监控队列深度等）。
func (m *Manager) Saver() *AsyncSaver {
	return m.saver
}

// Get 获取或懒加载玩家 Actor；并发首次访问时保证同一 roleID 只存在一个 Actor。
func (m *Manager) Get(ctx context.Context, roleID int64) (*Actor, error) {
	if roleID <= 0 {
		return nil, fmt.Errorf("invalid role id")
	}
	if !m.Owns(roleID) {
		return nil, fmt.Errorf("%w: role=%d owner=%d local=%d",
			ErrWrongShard, roleID, shard.ForRole(roleID, m.shardCount), m.shardID)
	}
	if v, ok := m.players.Load(roleID); ok {
		return v.(*Actor), nil
	}
	lockIndex := roleID % int64(len(m.loadLocks))
	if lockIndex < 0 {
		lockIndex = -lockIndex
	}
	loadLock := &m.loadLocks[lockIndex]
	loadLock.Lock()
	defer loadLock.Unlock()
	if v, ok := m.players.Load(roleID); ok {
		return v.(*Actor), nil
	}
	snap := repo.RoleSnapshot{Level: 1, Version: 1, OwnerEpoch: 1}
	if m.roles != nil {
		loaded, err := m.roles.Acquire(ctx, roleID)
		if err != nil {
			return nil, err
		}
		snap = loaded
	}
	p := New(roleID, snap, m.roles)
	actual, loaded := m.players.LoadOrStore(roleID, p)
	if loaded {
		// 另一 goroutine 抢先完成 LoadOrStore，关闭本 goroutine 多建的 Actor 与邮箱。
		p.Close()
		return actual.(*Actor), nil
	}
	return p, nil
}

// Owns reports whether this process is the configured single writer for role.
func (m *Manager) Owns(roleID int64) bool {
	return m.shardCount <= 1 || shard.ForRole(roleID, m.shardCount) == m.shardID
}

// HandleMsg 经 Actor 邮箱串行处理 Gate 转发的协议帧。
func (m *Manager) HandleMsg(ctx context.Context, roleID int64, cmd, act uint16, payload []byte) ([]byte, error) {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return nil, err
	}
	mutating := MutatingAct(cmd, act)
	if mutating && m.saver != nil {
		m.saver.Remove(roleID)
	}
	resp, err := pl.Invoke(ctx, func(a *Actor) ([]byte, error) {
		before := a.snapshotUnsafe()
		body, err := a.Handle(ctx, cmd, act, payload)
		if err != nil || !mutating {
			return body, err
		}
		if err := a.saveUnsafe(ctx); err != nil {
			a.restoreUnsafe(before)
			return nil, err
		}
		return body, nil
	})
	m.dropIfStale(roleID, pl, err)
	return resp, err
}

// WithPlayer serializes an operation with protocol handling and persistence.
func (m *Manager) WithPlayer(ctx context.Context, roleID int64, fn func(*Actor) error) error {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return err
	}
	_, err = pl.Invoke(ctx, func(a *Actor) ([]byte, error) {
		return nil, fn(a)
	})
	m.dropIfStale(roleID, pl, err)
	return err
}

// WithPlayerSaved serializes a mutation and its CAS persistence. If the write
// fails, the in-memory actor is restored before another mailbox task can run.
func (m *Manager) WithPlayerSaved(ctx context.Context, roleID int64, fn func(*Actor) error) error {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if m.saver != nil {
		m.saver.Remove(roleID)
	}
	_, err = pl.Invoke(ctx, func(a *Actor) ([]byte, error) {
		before := a.snapshotUnsafe()
		if err := fn(a); err != nil {
			a.restoreUnsafe(before)
			return nil, err
		}
		if err := a.saveUnsafe(ctx); err != nil {
			a.restoreUnsafe(before)
			return nil, err
		}
		return nil, nil
	})
	m.dropIfStale(roleID, pl, err)
	return err
}

func (m *Manager) dropIfStale(roleID int64, actor *Actor, err error) {
	if errors.Is(err, repo.ErrStaleRole) && m.players.CompareAndDelete(roleID, actor) {
		actor.Close()
	}
}

// WithPlayerOutbox atomically persists a mutation and its projection event.
func (m *Manager) WithPlayerOutbox(
	ctx context.Context,
	roleID int64,
	fn func(*Actor) (repo.OutboxEvent, error),
) error {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if m.saver != nil {
		m.saver.Remove(roleID)
	}
	_, err = pl.Invoke(ctx, func(a *Actor) ([]byte, error) {
		before := a.snapshotUnsafe()
		event, err := fn(a)
		if err != nil {
			a.restoreUnsafe(before)
			return nil, err
		}
		if err := a.saveWithOutboxUnsafe(ctx, event); err != nil {
			a.restoreUnsafe(before)
			return nil, err
		}
		return nil, nil
	})
	m.dropIfStale(roleID, pl, err)
	return err
}

// ScheduleSave 异步或同步落库（由 PersistConfig 决定）。
func (m *Manager) ScheduleSave(roleID int64) {
	v, ok := m.players.Load(roleID)
	if !ok {
		return
	}
	pl := v.(*Actor)
	if m.saver != nil {
		m.saver.Schedule(pl)
		return
	}
	_ = pl.Save(context.Background())
}

// SaveNow 立即落库（支付、拍卖等关键路径）。
func (m *Manager) SaveNow(ctx context.Context, roleID int64) error {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return err
	}
	var saveErr error
	if m.saver != nil {
		saveErr = m.saver.FlushNow(ctx, pl)
	} else {
		saveErr = pl.Save(ctx)
	}
	m.dropIfStale(roleID, pl, saveErr)
	return saveErr
}

func (m *Manager) SaveNowWithOutbox(ctx context.Context, roleID int64, event repo.OutboxEvent) error {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if m.saver != nil {
		m.saver.Remove(pl.ID)
	}
	err = pl.SaveWithOutbox(ctx, event)
	m.dropIfStale(roleID, pl, err)
	return err
}

// Logout 玩家下线：落库后从内存卸载，释放 Actor 与其邮箱 goroutine。
func (m *Manager) Logout(ctx context.Context, roleID int64) error {
	v, ok := m.players.Load(roleID)
	if !ok {
		return nil
	}
	a := v.(*Actor)
	var err error
	if m.saver != nil {
		err = m.saver.FlushNow(ctx, a)
	} else {
		err = a.Save(ctx)
	}
	if err != nil {
		m.dropIfStale(roleID, a, err)
		return err
	}
	if m.players.CompareAndDelete(roleID, a) {
		a.Close()
	}
	return nil
}

// evictLoop 后台定期淘汰空闲玩家，防止 Actor 与 goroutine 无限累积。
func (m *Manager) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-idleTimeout).Unix()
			m.players.Range(func(k, v any) bool {
				a := v.(*Actor)
				if a.LastActive() < cutoff {
					saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					var err error
					if m.saver != nil {
						err = m.saver.FlushNow(saveCtx, a)
					} else {
						err = a.Save(saveCtx)
					}
					cancel()
					if err == nil && a.LastActive() < cutoff && m.players.CompareAndDelete(k, a) {
						a.Close()
					} else {
						m.dropIfStale(k.(int64), a, err)
					}
				}
				return true
			})
		}
	}
}

// FlushAll 关停时把所有待落库玩家同步写入 DB，避免丢数据。
func (m *Manager) FlushAll(ctx context.Context) {
	if m.saver != nil {
		m.saver.FlushPending(ctx)
	}
}

// StopAndFlush stops background loops, waits for in-flight flushes, persists
// remaining actors, and closes all mailboxes.
func (m *Manager) StopAndFlush(ctx context.Context) {
	m.cancel()
	m.wg.Wait()
	m.FlushAll(ctx)
	m.players.Range(func(k, v any) bool {
		a := v.(*Actor)
		if m.players.CompareAndDelete(k, a) {
			_ = a.Save(ctx)
			a.Close()
		}
		return true
	})
}

// ApplyGrant applies a signed economy delta exactly once when source is set.
func (m *Manager) ApplyGrant(ctx context.Context, roleID int64, bundle grant.Bundle, source string) (*Actor, bool, error) {
	pl, err := m.Get(ctx, roleID)
	if err != nil {
		return nil, false, err
	}
	applied, err := pl.ApplyGrant(ctx, bundle, source)
	m.dropIfStale(roleID, pl, err)
	return pl, applied, err
}

// Online 返回当前内存中的在线 Actor 数（近似，用于监控）。
func (m *Manager) Online() int {
	n := 0
	m.players.Range(func(_, _ any) bool { n++; return true })
	return n
}

// MutatingAct 判断 CmdGame 下该 Act 是否会修改玩家内存状态（需 ScheduleSave）。
//
// CmdGame Act 与是否写库对照（新增 Act 时请同步更新本表与 persist_test）：
//
//	ActPlayerData(2)   false — 只读玩家快照
//	ActSkillList(3)    false — 只读技能列表
//	ActSkillUpgrade(4) true  — 升级技能，修改 gold/skills
//	ActQuestList(5)    false — 只读任务列表
//	ActQuestAccept(6)  true  — 接取任务，修改 quests
//
// 副本通关、公会、拍卖、支付发奖等走 Game HTTP /internal/*，由 Handler 显式 SaveNow/ScheduleSave。
func MutatingAct(cmd, act uint16) bool {
	if cmd != protocol.CmdGame {
		return false
	}
	switch act {
	case protocol.ActSkillUpgrade, protocol.ActQuestAccept:
		return true
	case protocol.ActPlayerData, protocol.ActSkillList, protocol.ActQuestList:
		return false
	default:
		// 未知 Act 默认不触发异步落库，避免误写；新增写操作 Act 须加入上方 true 分支。
		return false
	}
}
