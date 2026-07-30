// Package db 封装 PostgreSQL 连接池。
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool 根据 DSN 创建 pgx 连接池。
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 20
	}
	if cfg.MinConns <= 0 {
		cfg.MinConns = 2
	}
	if cfg.MaxConnLifetime <= 0 {
		cfg.MaxConnLifetime = 30 * time.Minute
	}
	if cfg.MaxConnIdleTime <= 0 {
		cfg.MaxConnIdleTime = 5 * time.Minute
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
