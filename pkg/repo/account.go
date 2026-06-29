package repo

import (
	"context"
	"fmt"

	"newgame/pkg/auth"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AccountRepo 账号与区服角色的创建、登录校验。
type AccountRepo struct {
	pool *pgxpool.Pool
}

// NewAccountRepo 创建账号仓库。
func NewAccountRepo(pool *pgxpool.Pool) *AccountRepo {
	return &AccountRepo{pool: pool}
}

// Authenticate 校验用户名密码；账号不存在时自动注册并返回新 accountID。
//
// 参数:
//   - ctx: 数据库上下文
//   - username: 登录用户名，不可为空
//   - password: 明文密码，不可为空
//
// 返回:
//   - int64: 账号 ID（accounts.id）
//   - error: 密码错误、参数为空或数据库错误
func (r *AccountRepo) Authenticate(ctx context.Context, username, password string) (int64, error) {
	if username == "" || password == "" {
		return 0, fmt.Errorf("username and password required")
	}
	var id int64
	var hash string
	err := r.pool.QueryRow(ctx,
		`SELECT id, password_hash FROM accounts WHERE username = $1`, username,
	).Scan(&id, &hash)
	if err == pgx.ErrNoRows {
		newHash, err := auth.HashPassword(password)
		if err != nil {
			return 0, err
		}
		err = r.pool.QueryRow(ctx,
			`INSERT INTO accounts (username, password_hash) VALUES ($1, $2) RETURNING id`,
			username, newHash,
		).Scan(&id)
		return id, err
	}
	if err != nil {
		return 0, err
	}
	if !auth.CheckPassword(hash, password) {
		return 0, fmt.Errorf("invalid password")
	}
	return id, nil
}

// Role 区服角色基础信息（不含 snapshot 详情）。
type Role struct {
	ID        int64  // 角色 ID
	AccountID int64  // 所属账号 ID
	ZoneID    int32  // 区服 ID
	Name      string // 角色名（通常与用户名相同）
	Level     int32  // 等级（表字段，详细数据在 snapshot）
}

// GetOrCreateRole 获取账号在指定区服的角色，不存在则创建。
//
// 参数:
//   - ctx: 数据库上下文
//   - accountID: 账号 ID
//   - zoneID: 目标区服 ID
//   - username: 用于生成角色名，最长截断至 32 字符
//
// 返回:
//   - Role: 角色信息；新角色 ID 规则为 accountID*10000 + zoneID
//   - error: 数据库错误
func (r *AccountRepo) GetOrCreateRole(ctx context.Context, accountID int64, zoneID int32, username string) (Role, error) {
	var role Role
	err := r.pool.QueryRow(ctx,
		`SELECT id, account_id, zone_id, name, level FROM roles
		 WHERE account_id = $1 AND zone_id = $2`,
		accountID, zoneID,
	).Scan(&role.ID, &role.AccountID, &role.ZoneID, &role.Name, &role.Level)
	if err == nil {
		return role, nil
	}
	if err != pgx.ErrNoRows {
		return Role{}, err
	}
	roleID := accountID*10000 + int64(zoneID)
	name := username
	if len(name) > 32 {
		name = name[:32]
	}
	err = r.pool.QueryRow(ctx,
		`INSERT INTO roles (id, account_id, zone_id, name, level)
		 VALUES ($1, $2, $3, $4, 1)
		 ON CONFLICT (zone_id, name) DO UPDATE SET account_id = EXCLUDED.account_id
		 RETURNING id, account_id, zone_id, name, level`,
		roleID, accountID, zoneID, name,
	).Scan(&role.ID, &role.AccountID, &role.ZoneID, &role.Name, &role.Level)
	return role, err
}
