package repo

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Guild 公会表 guilds 一行。
type Guild struct {
	ID     int64  // 公会 ID
	ZoneID int32  // 所属区服
	Name   string // 公会名
}

// GuildRepo 公会与成员关系 guild_members 的读写。
type GuildRepo struct {
	pool *pgxpool.Pool
}

func NewGuildRepo(pool *pgxpool.Pool) *GuildRepo {
	return &GuildRepo{pool: pool}
}

// Get 按公会 ID 查询基础信息。
func (r *GuildRepo) Get(ctx context.Context, guildID int64) (Guild, error) {
	var g Guild
	err := r.pool.QueryRow(ctx,
		`SELECT id, zone_id, name FROM guilds WHERE id = $1`, guildID,
	).Scan(&g.ID, &g.ZoneID, &g.Name)
	return g, err
}

// ListMembers 返回公会内所有成员 role_id。
func (r *GuildRepo) ListMembers(ctx context.Context, guildID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx, `SELECT role_id FROM guild_members WHERE guild_id = $1`, guildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Join 角色加入公会；已存在则 ON CONFLICT DO NOTHING。
func (r *GuildRepo) Join(ctx context.Context, guildID, roleID int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO guild_members (guild_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		guildID, roleID,
	)
	return err
}

// RoleGuild 查询角色当前所属公会；未加入时返回零值 Guild 且无 error。
func (r *GuildRepo) RoleGuild(ctx context.Context, roleID int64) (Guild, error) {
	var g Guild
	err := r.pool.QueryRow(ctx,
		`SELECT g.id, g.zone_id, g.name FROM guilds g
		 JOIN guild_members m ON m.guild_id = g.id WHERE m.role_id = $1 LIMIT 1`,
		roleID,
	).Scan(&g.ID, &g.ZoneID, &g.Name)
	if err == pgx.ErrNoRows {
		return Guild{}, nil
	}
	return g, err
}
