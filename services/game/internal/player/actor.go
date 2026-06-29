// Package player 实现 Game 服单玩家 Actor：内存态、协议处理与持久化。
package player

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"newgame/pkg/actor"
	"newgame/pkg/grant"
	"newgame/pkg/protocol"
	"newgame/pkg/repo"
	"newgame/services/game/internal/bag"
	"newgame/services/game/internal/quest"
	"newgame/services/game/internal/skill"
)

// Actor 单个在线玩家的游戏实体，对应一条 role 记录的运行时视图。
type Actor struct {
	ID         int64          // 角色 ID
	Level      int32          // 等级
	Gold       int64          // 金币
	Inv        bag.Inventory  // 背包
	Skills     skill.Book     // 技能书
	Quests     quest.State    // 任务状态
	GuildID    int64          // 公会 ID
	mailbox    *actor.Mailbox // 串行消息邮箱（Actor 模型）
	repo       *repo.RoleRepo // 持久化；nil 时不写库
	lastActive atomic.Int64   // 最近活跃 Unix 秒，供空闲淘汰
}

// New 从数据库快照构造玩家 Actor。
//
// 参数:
//   - id: 角色 ID
//   - snap: 自 repo.Load 得到的快照
//   - roles: 角色仓库；nil 时 Save 为空操作
func New(id int64, snap repo.RoleSnapshot, roles *repo.RoleRepo) *Actor {
	inv := snap.Bag
	if inv == nil {
		inv = bag.New()
	}
	a := &Actor{
		ID:      id,
		Level:   snap.Level,
		Gold:    snap.Gold,
		Inv:     inv,
		Skills:  skill.FromSnapshot(snap),
		Quests:  quest.FromSnapshot(snap),
		GuildID: snap.GuildID,
		mailbox: actor.NewMailbox(128),
		repo:    roles,
	}
	a.touch()
	return a
}

// touch 更新最近活跃时间。
func (p *Actor) touch() { p.lastActive.Store(time.Now().Unix()) }

// LastActive 返回最近活跃 Unix 秒。
func (p *Actor) LastActive() int64 { return p.lastActive.Load() }

// Close 关闭玩家邮箱，停止其后台 goroutine（淘汰/下线时调用）。
func (p *Actor) Close() { p.mailbox.Close() }

// PlayerID 返回角色 ID，实现 actor.Player 接口。
func (p *Actor) PlayerID() int64 { return p.ID }

// Handle 处理 Gate 转发的游戏协议帧（CmdGame 及子 Act）。
// 状态变更由调用方 ScheduleSave；本方法内不再同步落库。
func (p *Actor) Handle(ctx context.Context, cmd, act uint16, payload []byte) ([]byte, error) {
	_ = ctx
	if cmd != protocol.CmdGame {
		return json.Marshal(map[string]any{"role_id": p.ID, "echo": string(payload)})
	}
	switch act {
	case protocol.ActPlayerData:
		return json.Marshal(map[string]any{
			"role_id":  p.ID,
			"level":    p.Level,
			"gold":     p.Gold,
			"bag":      p.Inv,
			"skills":   p.Skills.List(),
			"quests":   p.Quests.List(),
			"guild_id": p.GuildID,
		})
	case protocol.ActSkillList:
		return json.Marshal(map[string]any{"skills": p.Skills.List()})
	case protocol.ActSkillUpgrade:
		var req struct {
			SkillID string `json:"skill_id"` // 技能 ID，空则默认 slash
		}
		_ = json.Unmarshal(payload, &req)
		if req.SkillID == "" {
			req.SkillID = "slash"
		}
		lv := p.Skills.Upgrade(req.SkillID)
		return json.Marshal(map[string]any{"skill_id": req.SkillID, "level": lv})
	case protocol.ActQuestList:
		return json.Marshal(map[string]any{"quests": p.Quests.List()})
	case protocol.ActQuestAccept:
		var req struct {
			QuestID string `json:"quest_id"` // 任务 ID，空则默认 main_1
		}
		_ = json.Unmarshal(payload, &req)
		if req.QuestID == "" {
			req.QuestID = "main_1"
		}
		err := p.Quests.Accept(req.QuestID)
		if err != nil {
			return json.Marshal(map[string]any{"code": 1001, "message": err.Error()})
		}
		return json.Marshal(map[string]any{"code": 0, "quest_id": req.QuestID})
	default:
		return json.Marshal(map[string]any{
			"role_id": p.ID,
			"cmd":     cmd,
			"act":     act,
			"echo":    string(payload),
		})
	}
}

// Invoke 经邮箱串行执行 fn，保证同一玩家协议不并发写状态。
func (p *Actor) Invoke(ctx context.Context, fn func(*Actor) ([]byte, error)) ([]byte, error) {
	p.touch()
	return p.mailbox.Call(ctx, func() ([]byte, error) {
		return fn(p)
	})
}

// Snapshot 导出当前内存态，供 Save 与异步落库使用。
func (p *Actor) Snapshot() repo.RoleSnapshot {
	qs, qp := p.Quests.ToSnapshotFields()
	return repo.RoleSnapshot{
		Level:     p.Level,
		Gold:      p.Gold,
		Bag:       p.Inv,
		Skills:    p.Skills.ToMap(),
		Quests:    qs,
		QuestProg: qp,
		GuildID:   p.GuildID,
	}
}

// Save 将当前 Actor 状态写入 Postgres roles 表。
//
// 参数:
//   - ctx: 更新上下文
//
// 返回: repo 为 nil 时直接返回 nil
func (p *Actor) Save(ctx context.Context) error {
	if p.repo == nil {
		return nil
	}
	return p.repo.Save(ctx, p.ID, p.Snapshot())
}

// AddGold 增加金币（amount 必须 > 0 才生效）。
//
// 参数:
//   - amount: 增加的金币数量
func (p *Actor) AddGold(amount int64) {
	if amount > 0 {
		p.Gold += amount
	}
}

// SpendGold 扣除金币。
//
// 参数:
//   - amount: 扣除数量，必须 > 0
//
// 返回: 余额不足或 amount<=0 时 false；成功扣除时 true
func (p *Actor) SpendGold(amount int64) bool {
	if amount <= 0 || p.Gold < amount {
		return false
	}
	p.Gold -= amount
	return true
}

// LevelUp 等级 +1。
func (p *Actor) LevelUp() {
	p.Level++
}

// SetGuild 设置所属公会 ID。
//
// 参数:
//   - id: 公会 ID
func (p *Actor) SetGuild(id int64) {
	p.GuildID = id
}

// ApplyGrant 将奖励包应用到玩家并 Save。
//
// 参数:
//   - ctx: 持久化上下文
//   - b: 金币与道具包
func (p *Actor) ApplyGrant(ctx context.Context, b grant.Bundle) error {
	if b.Gold > 0 {
		p.AddGold(b.Gold)
	}
	for item, n := range b.Items {
		p.Inv.Add(item, n)
	}
	return p.Save(ctx)
}
