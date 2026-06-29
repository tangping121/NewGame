// Package repo 提供各业务表的 Postgres 访问层（账号、角色、支付、邮件、公会、拍卖等）。
package repo

import (
	"context"
	"encoding/json"

	"newgame/pkg/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RoleRepo 角色运行时数据（等级、背包、任务等）的读写。
//
// 支持单库与分库：poolFor 按 role_id 解析目标连接池（分库时路由到对应 PG 实例）。
type RoleRepo struct {
	poolFor func(roleID int64) *pgxpool.Pool
}

// NewRoleRepo 创建单库角色仓库。
//
// 参数:
//   - pool: 已初始化的 pgx 连接池
func NewRoleRepo(pool *pgxpool.Pool) *RoleRepo {
	return &RoleRepo{poolFor: func(int64) *pgxpool.Pool { return pool }}
}

// NewRoleRepoSharded 创建分库角色仓库，按 role_id 路由到对应 Postgres 实例。
//
// 参数:
//   - sp: 已初始化的分库连接池
func NewRoleRepoSharded(sp *db.ShardedPool) *RoleRepo {
	return &RoleRepo{poolFor: sp.ForRole}
}

// RoleSnapshot 角色内存态快照，序列化后存入 roles.snapshot JSONB 字段。
type RoleSnapshot struct {
	Level     int32            `json:"level"`      // 等级
	Gold      int64            `json:"gold"`       // 金币
	Bag       map[string]int32 `json:"bag"`        // 背包：item_id -> 数量
	Skills    map[string]int32 `json:"skills"`     // 技能：skill_id -> 等级
	Quests    map[string]int32 `json:"quests"`     // 任务状态：quest_id -> status
	QuestProg map[string]int32 `json:"quest_prog"` // 任务进度：quest_id -> 计数
	GuildID   int64            `json:"guild_id"`   // 所属公会 ID，0 表示未加入
}

// Load 从数据库加载角色快照。
//
// 参数:
//   - ctx: 查询上下文
//   - roleID: 角色 ID
//
// 返回:
//   - RoleSnapshot: 不存在时返回 Level=1 的默认快照
//   - error: 数据库错误（非 ErrNoRows）
func (r *RoleRepo) Load(ctx context.Context, roleID int64) (RoleSnapshot, error) {
	var snap RoleSnapshot
	var raw []byte
	err := r.poolFor(roleID).QueryRow(ctx,
		`SELECT level, snapshot FROM roles WHERE id = $1`, roleID,
	).Scan(&snap.Level, &raw)
	if err == pgx.ErrNoRows {
		return RoleSnapshot{Level: 1}, nil
	}
	if err != nil {
		return RoleSnapshot{}, err
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &snap)
	}
	if snap.Level == 0 {
		snap.Level = 1
	}
	if snap.Bag == nil {
		snap.Bag = map[string]int32{}
	}
	if snap.Skills == nil {
		snap.Skills = map[string]int32{}
	}
	if snap.Quests == nil {
		snap.Quests = map[string]int32{}
	}
	if snap.QuestProg == nil {
		snap.QuestProg = map[string]int32{}
	}
	return snap, nil
}

// Save 将角色快照写回数据库。
//
// 参数:
//   - ctx: 更新上下文
//   - roleID: 角色 ID
//   - snap: 待持久化的完整快照
func (r *RoleRepo) Save(ctx context.Context, roleID int64, snap RoleSnapshot) error {
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	_, err = r.poolFor(roleID).Exec(ctx,
		`UPDATE roles SET level = $2, snapshot = $3, updated_at = NOW() WHERE id = $1`,
		roleID, snap.Level, raw,
	)
	return err
}
