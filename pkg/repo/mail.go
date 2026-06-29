package repo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Mail 邮件表一行记录；is_read 与 claimed 相互独立。
type Mail struct {
	ID        int64     // 邮件 ID
	RoleID    int64     // 收件人角色 ID
	Title     string    // 标题
	Content   string    // 正文
	Items     string    // 附件奖励串，如 gold:100,potion:1；空表示无附件
	Read      bool      // 是否已读
	Claimed   bool      // 附件是否已领取
	CreatedAt time.Time // 创建时间
}

// MailRepo 邮件 CRUD 与已读/领取状态更新。
type MailRepo struct {
	pool *pgxpool.Pool
}

// NewMailRepo 创建邮件仓库。
func NewMailRepo(pool *pgxpool.Pool) *MailRepo {
	return &MailRepo{pool: pool}
}

// Insert 发送一封新邮件。
//
// 参数:
//   - ctx: 上下文
//   - m: 邮件内容；RoleID、Title 等必填字段由调用方填充
func (r *MailRepo) Insert(ctx context.Context, m Mail) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO mails (role_id, title, content, items) VALUES ($1, $2, $3, $4)`,
		m.RoleID, m.Title, m.Content, m.Items,
	)
	return err
}

// List 按角色查询邮件列表，按 id 降序。
//
// 参数:
//   - roleID: 收件人
//   - limit: 条数上限；<=0 默认 50
func (r *MailRepo) List(ctx context.Context, roleID int64, limit int) ([]Mail, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, role_id, title, content, items, is_read, claimed, created_at
		 FROM mails WHERE role_id = $1 ORDER BY id DESC LIMIT $2`, roleID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mail
	for rows.Next() {
		var m Mail
		if err := rows.Scan(&m.ID, &m.RoleID, &m.Title, &m.Content, &m.Items, &m.Read, &m.Claimed, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Get 按邮件 ID 与角色 ID 查询单封（防止越权）。
func (r *MailRepo) Get(ctx context.Context, mailID, roleID int64) (Mail, error) {
	var m Mail
	err := r.pool.QueryRow(ctx,
		`SELECT id, role_id, title, content, items, is_read, claimed, created_at
		 FROM mails WHERE id = $1 AND role_id = $2`, mailID, roleID,
	).Scan(&m.ID, &m.RoleID, &m.Title, &m.Content, &m.Items, &m.Read, &m.Claimed, &m.CreatedAt)
	return m, err
}

// ListUnclaimed 列出有附件且未领取的邮件，供 claim-all 使用。
func (r *MailRepo) ListUnclaimed(ctx context.Context, roleID int64, limit int) ([]Mail, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, role_id, title, content, items, is_read, claimed, created_at
		 FROM mails WHERE role_id = $1 AND claimed = false AND items <> '' ORDER BY id LIMIT $2`,
		roleID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mail
	for rows.Next() {
		var m Mail
		if err := rows.Scan(&m.ID, &m.RoleID, &m.Title, &m.Content, &m.Items, &m.Read, &m.Claimed, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MarkClaimedBatch 批量标记已领取（不自动已读）。
func (r *MailRepo) MarkClaimedBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.pool.Exec(ctx, `UPDATE mails SET claimed = true WHERE id = ANY($1)`, ids)
	return err
}

// MarkClaimed 单封邮件标记已领取。
func (r *MailRepo) MarkClaimed(ctx context.Context, mailID int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE mails SET claimed = true WHERE id = $1`, mailID,
	)
	return err
}

// MarkRead 单封邮件标记已读（不影响 claimed）。
func (r *MailRepo) MarkRead(ctx context.Context, mailID, roleID int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE mails SET is_read = true WHERE id = $1 AND role_id = $2`, mailID, roleID,
	)
	return err
}

// MarkReadAll 将角色所有未读邮件标为已读。
//
// 返回: 实际更新行数
func (r *MailRepo) MarkReadAll(ctx context.Context, roleID int64) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE mails SET is_read = true WHERE role_id = $1 AND is_read = false`, roleID,
	)
	return tag.RowsAffected(), err
}

// CountUnreadUnclaimed 统计未读数与未领取（有附件）数，供红点展示。
func (r *MailRepo) CountUnreadUnclaimed(ctx context.Context, roleID int64) (unread, unclaimed int64, err error) {
	err = r.pool.QueryRow(ctx,
		`SELECT
		 COUNT(*) FILTER (WHERE is_read = false)::bigint,
		 COUNT(*) FILTER (WHERE claimed = false AND items <> '')::bigint
		 FROM mails WHERE role_id = $1`, roleID,
	).Scan(&unread, &unclaimed)
	return
}
