// Package db 封装 PostgreSQL 连接池。
package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool 根据 DSN 创建 pgx 连接池。
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
