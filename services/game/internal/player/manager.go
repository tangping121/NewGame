package player

import (
	"context"
	"sync"
	"time"

	"newgame/pkg/protocol"
	"newgame/pkg/repo"
)

// idleTimeout 玩家无活动超过此时长后被淘汰（落库 + 关闭邮箱）。
const idleTimeout = 30 * time.Minute

// Manager 在线玩家 Actor 缓存：同一 roleID 复用同一 Actor 实例。
type Manager struct {
	roles   *repo.RoleRepo // 角色持久化；nil 时使用默认空快照
	players sync.Map       // roleID -> *Actor
	saver   *AsyncSaver    // 异步落库；nil 时 ScheduleSave 退化为同步 Save
}

// NewManager 创建玩家管理器。
//
// 参数:
//   - roles: Postgres 角色仓库；可为 nil（纯内存模式）
//   - persist: 落库策略；Mode=async 时启动后台 flush
func NewManager(roles *repo.RoleRepo, persist PersistConfig) *Manager {
	m := &Manager{roles: roles}
	if roles != nil && persist.Mode == "async" {
		m.saver = NewAsyncSaver(roles, persist.Interval, persist.Concurrency)
		go m.saver.Run(context.Background())
	}
	go m.evictLoop()
	return m
}

// Saver 返回异步落库器（监控队列深度等）。
func (m *Manager) Saver() *AsyncSaver {
	return m.saver
}

// Get 获取或懒加载玩家 Actor。
func (m *Manager) Get(ctx context.Context, roleID int64) *Actor {
	if v, ok := m.players.Load(roleID); ok {
		return v.(*Actor)
	}
	snap := repo.RoleSnapshot{Level: 1, Gold: 0, Bag: nil}
	if m.roles != nil {
		if loaded, err := m.roles.Load(ctx, roleID); err == nil {
			snap = loaded
		}
	}
	p := New(roleID, snap, m.roles)
	m.players.Store(roleID, p)
	return p
}

// HandleMsg 经 Actor 邮箱串行处理 Gate 转发的协议帧。
func (m *Manager) HandleMsg(ctx context.Context, roleID int64, cmd, act uint16, payload []byte) ([]byte, error) {
	pl := m.Get(ctx, roleID)
	resp, err := pl.Invoke(ctx, func(a *Actor) ([]byte, error) {
		return a.Handle(ctx, cmd, act, payload)
	})
	if err != nil {
		return nil, err
	}
	if MutatingAct(cmd, act) {
		m.ScheduleSave(roleID)
	}
	return resp, nil
}

// ScheduleSave 异步或同步落库（由 PersistConfig 决定）。
func (m *Manager) ScheduleSave(roleID int64) {
	pl := m.Get(context.Background(), roleID)
	if m.saver != nil {
		m.saver.Schedule(pl)
		return
	}
	_ = pl.Save(context.Background())
}

// SaveNow 立即落库（支付、拍卖等关键路径）。
func (m *Manager) SaveNow(ctx context.Context, roleID int64) error {
	pl := m.Get(ctx, roleID)
	if m.saver != nil {
		return m.saver.FlushNow(ctx, pl)
	}
	return pl.Save(ctx)
}

// Logout 玩家下线：落库后从内存卸载，释放 Actor 与其邮箱 goroutine。
func (m *Manager) Logout(ctx context.Context, roleID int64) {
	v, ok := m.players.LoadAndDelete(roleID)
	if !ok {
		return
	}
	a := v.(*Actor)
	_ = a.Save(ctx) // 同步落库，确保下线数据不丢
	a.Close()
}

// evictLoop 后台定期淘汰空闲玩家，防止 Actor 与 goroutine 无限累积。
func (m *Manager) evictLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-idleTimeout).Unix()
		m.players.Range(func(k, v any) bool {
			a := v.(*Actor)
			if a.LastActive() < cutoff {
				if _, loaded := m.players.LoadAndDelete(k); loaded {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = a.Save(ctx)
					cancel()
					a.Close()
				}
			}
			return true
		})
	}
}

// Online 返回当前内存中的在线 Actor 数（近似，用于监控）。
func (m *Manager) Online() int {
	n := 0
	m.players.Range(func(_, _ any) bool { n++; return true })
	return n
}

// MutatingAct 判断协议是否会修改玩家状态（需 ScheduleSave）。
func MutatingAct(cmd, act uint16) bool {
	if cmd != protocol.CmdGame {
		return false
	}
	switch act {
	case protocol.ActSkillUpgrade, protocol.ActQuestAccept:
		return true
	default:
		return false
	}
}
