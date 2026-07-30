// Package repo 提供各业务表的 Postgres 访问层（账号、角色、支付、邮件、公会、拍卖等）。
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"newgame/pkg/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrRoleNotFound = errors.New("role not found")
	ErrStaleRole    = errors.New("stale role snapshot")
)

// RoleRepo 角色运行时数据（等级、背包、任务等）的读写。
//
// 支持单库与分库：poolFor 按 role_id 解析目标连接池（分库时路由到对应 PG 实例）。
type RoleRepo struct {
	poolFor func(roleID int64) *pgxpool.Pool
	pools   []*pgxpool.Pool
}

// NewRoleRepo 创建单库角色仓库。
//
// 参数:
//   - pool: 已初始化的 pgx 连接池
func NewRoleRepo(pool *pgxpool.Pool) *RoleRepo {
	return &RoleRepo{poolFor: func(int64) *pgxpool.Pool { return pool }, pools: []*pgxpool.Pool{pool}}
}

// NewRoleRepoSharded 创建分库角色仓库，按 role_id 路由到对应 Postgres 实例。
//
// 参数:
//   - sp: 已初始化的分库连接池
func NewRoleRepoSharded(sp *db.ShardedPool) *RoleRepo {
	return &RoleRepo{poolFor: sp.ForRole, pools: sp.All()}
}

// RoleSnapshot 角色内存态快照，序列化后存入 roles.snapshot JSONB 字段。
type RoleSnapshot struct {
	Level      int32            `json:"level"`      // 等级
	Gold       int64            `json:"gold"`       // 金币
	Bag        map[string]int32 `json:"bag"`        // 背包：item_id -> 数量
	Skills     map[string]int32 `json:"skills"`     // 技能：skill_id -> 等级
	Quests     map[string]int32 `json:"quests"`     // 任务状态：quest_id -> status
	QuestProg  map[string]int32 `json:"quest_prog"` // 任务进度：quest_id -> 计数
	GuildID    int64            `json:"guild_id"`   // 所属公会 ID，0 表示未加入
	Version    int64            `json:"-"`          // optimistic concurrency version
	OwnerEpoch int64            `json:"-"`          // shard ownership fencing token
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
		`SELECT level, snapshot, version, owner_epoch FROM roles WHERE id = $1`, roleID,
	).Scan(&snap.Level, &raw, &snap.Version, &snap.OwnerEpoch)
	if err == pgx.ErrNoRows {
		return RoleSnapshot{}, ErrRoleNotFound
	}
	if err != nil {
		return RoleSnapshot{}, err
	}
	return decodeRoleSnapshot(roleID, snap, raw)
}

// Acquire fences any previous in-memory owner and returns the latest snapshot.
// A stale process can no longer save after a newer process acquires the role.
func (r *RoleRepo) Acquire(ctx context.Context, roleID int64) (RoleSnapshot, error) {
	var snap RoleSnapshot
	var raw []byte
	err := r.poolFor(roleID).QueryRow(ctx,
		`UPDATE roles
		    SET owner_epoch = owner_epoch + 1, updated_at = NOW()
		  WHERE id = $1
		  RETURNING level, snapshot, version, owner_epoch`,
		roleID,
	).Scan(&snap.Level, &raw, &snap.Version, &snap.OwnerEpoch)
	if err == pgx.ErrNoRows {
		return RoleSnapshot{}, ErrRoleNotFound
	}
	if err != nil {
		return RoleSnapshot{}, err
	}
	return decodeRoleSnapshot(roleID, snap, raw)
}

func decodeRoleSnapshot(roleID int64, snap RoleSnapshot, raw []byte) (RoleSnapshot, error) {
	if len(raw) > 0 {
		version, ownerEpoch := snap.Version, snap.OwnerEpoch
		if err := json.Unmarshal(raw, &snap); err != nil {
			return RoleSnapshot{}, fmt.Errorf("decode role %d snapshot: %w", roleID, err)
		}
		snap.Version, snap.OwnerEpoch = version, ownerEpoch
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
	if snap.Version <= 0 {
		snap.Version = 1
	}
	if snap.OwnerEpoch <= 0 {
		snap.OwnerEpoch = 1
	}
	return snap, nil
}

// Save 将角色快照写回数据库。
//
// 参数:
//   - ctx: 更新上下文
//   - roleID: 角色 ID
//   - snap: 待持久化的完整快照
func (r *RoleRepo) Save(ctx context.Context, roleID int64, snap RoleSnapshot) (int64, error) {
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, err
	}
	var version int64
	err = r.poolFor(roleID).QueryRow(ctx,
		`UPDATE roles
		    SET level = $2, snapshot = $3, version = version + 1, updated_at = NOW()
		  WHERE id = $1 AND version = $4 AND owner_epoch = $5
		  RETURNING version`,
		roleID, snap.Level, raw, snap.Version, snap.OwnerEpoch,
	).Scan(&version)
	if err == pgx.ErrNoRows {
		return 0, ErrStaleRole
	}
	return version, err
}

// SaveIdempotent applies a full snapshot and records its immutable source in
// the same database transaction. A duplicate source is a successful no-op.
func (r *RoleRepo) SaveIdempotent(
	ctx context.Context,
	roleID int64,
	source, kind string,
	snap RoleSnapshot,
	payload any,
) (version int64, applied bool, err error) {
	if source == "" {
		version, err = r.Save(ctx, roleID, snap)
		return version, err == nil, err
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, false, err
	}
	entry, err := json.Marshal(payload)
	if err != nil {
		return 0, false, err
	}
	tx, err := r.poolFor(roleID).Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var ledgerID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO economy_ledger (role_id, source, kind, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (role_id, source) DO NOTHING
		 RETURNING id`,
		roleID, source, kind, entry,
	).Scan(&ledgerID)
	if err == pgx.ErrNoRows {
		return snap.Version, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	err = tx.QueryRow(ctx,
		`UPDATE roles
		    SET level = $2, snapshot = $3, version = version + 1, updated_at = NOW()
		  WHERE id = $1 AND version = $4 AND owner_epoch = $5
		  RETURNING version`,
		roleID, snap.Level, raw, snap.Version, snap.OwnerEpoch,
	).Scan(&version)
	if err == pgx.ErrNoRows {
		return 0, false, ErrStaleRole
	}
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, err
	}
	return version, true, nil
}

// Ensure creates the runtime role row on its owning data shard. It is safe to
// call after every login and never overwrites an existing snapshot.
func (r *RoleRepo) Ensure(ctx context.Context, role Role) error {
	_, err := r.poolFor(role.ID).Exec(ctx,
		`INSERT INTO roles
		    (id, account_id, zone_id, name, level, snapshot, version, owner_epoch)
		 VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 1, 1)
		 ON CONFLICT (id) DO NOTHING`,
		role.ID, role.AccountID, role.ZoneID, role.Name, role.Level,
	)
	return err
}
