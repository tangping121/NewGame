package repo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ChatMessage struct {
	ID        int64
	Channel   string
	RoleID    int64
	Text      string
	CreatedAt time.Time
}

type SocialRepo struct {
	pool *pgxpool.Pool
}

func NewSocialRepo(pool *pgxpool.Pool) *SocialRepo {
	return &SocialRepo{pool: pool}
}

func (r *SocialRepo) AddFriend(ctx context.Context, roleID, friendID int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO friends (role_id, friend_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		roleID, friendID,
	)
	return err
}

func (r *SocialRepo) ListFriends(ctx context.Context, roleID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx, `SELECT friend_id FROM friends WHERE role_id = $1`, roleID)
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

func (r *SocialRepo) InsertChat(ctx context.Context, channel string, roleID int64, text string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO chat_messages (channel, role_id, text) VALUES ($1, $2, $3)`,
		channel, roleID, text,
	)
	return err
}

func (r *SocialRepo) RecentChat(ctx context.Context, channel string, limit int) ([]ChatMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, channel, role_id, text, created_at FROM chat_messages
		 WHERE channel = $1 ORDER BY id DESC LIMIT $2`, channel, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChatMessage
	for rows.Next() {
		var m ChatMessage
		if err := rows.Scan(&m.ID, &m.Channel, &m.RoleID, &m.Text, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
