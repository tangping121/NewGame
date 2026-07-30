package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dir := flag.String("dir", "./deploy", "directory containing init.sql and migrate_*.sql")
	dsn := flag.String("dsn", os.Getenv("NG_POSTGRES"), "central PostgreSQL DSN")
	shards := flag.String("shards", os.Getenv("NG_POSTGRES_SHARDS"), "comma-separated shard PostgreSQL DSNs")
	shardCount := flag.Int("shard-count", envInt("NG_SHARD_COUNT", 1), "logical Game shard count")
	flag.Parse()

	dsns := uniqueDSNs(*dsn, *shards)
	if len(dsns) == 0 {
		panic("at least one database DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for i, databaseDSN := range dsns {
		if err := run(ctx, databaseDSN, *dir); err != nil {
			panic(fmt.Errorf("database %d: %w", i, err))
		}
	}
	centralDSN := strings.TrimSpace(*dsn)
	if centralDSN == "" {
		centralDSN = dsns[0]
	}
	roleDSNs := splitCSV(*shards)
	if len(roleDSNs) == 0 {
		roleDSNs = []string{centralDSN}
	}
	if err := syncRoleDirectory(ctx, centralDSN, roleDSNs, *shardCount); err != nil {
		panic(fmt.Errorf("sync role directory: %w", err))
	}
}

func uniqueDSNs(central, shards string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, value := range append([]string{central}, splitCSV(shards)...) {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func splitCSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	number, err := strconv.Atoi(value)
	if err != nil || number <= 0 {
		panic(fmt.Sprintf("%s must be a positive integer", name))
	}
	return number
}

func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type migration struct {
		name    string
		version int
	}
	var migrations []migration
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || (name != "init.sql" &&
			!(strings.HasPrefix(name, "migrate_") && strings.HasSuffix(name, ".sql"))) {
			continue
		}
		version := 0
		if name != "init.sql" {
			raw := strings.TrimSuffix(strings.TrimPrefix(name, "migrate_v"), ".sql")
			var parseErr error
			version, parseErr = strconv.Atoi(raw)
			if parseErr != nil || version <= 0 {
				return nil, fmt.Errorf("migration %q must use migrate_vN.sql naming", name)
			}
		}
		migrations = append(migrations, migration{name: name, version: version})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	files := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		files = append(files, migration.name)
	}
	return files, nil
}

type directoryRole struct {
	id        int64
	accountID int64
	zoneID    int32
	name      string
	level     int32
	createdAt time.Time
	updatedAt time.Time
}

const upsertDirectoryRoleSQL = `
	INSERT INTO role_directory
	    (id, account_id, zone_id, name, level, shard_id, created_at, updated_at)
	 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (id) DO UPDATE
	  SET account_id = EXCLUDED.account_id,
	      zone_id = EXCLUDED.zone_id,
	      name = EXCLUDED.name,
	      level = EXCLUDED.level,
	      shard_id = EXCLUDED.shard_id,
	      updated_at = GREATEST(role_directory.updated_at, EXCLUDED.updated_at)`

func syncRoleDirectory(ctx context.Context, centralDSN string, roleDSNs []string, shardCount int) error {
	if shardCount <= 0 {
		return fmt.Errorf("shard count must be positive")
	}
	central, err := pgxpool.New(ctx, centralDSN)
	if err != nil {
		return err
	}
	defer central.Close()
	if err := central.Ping(ctx); err != nil {
		return err
	}
	for sourceIndex, sourceDSN := range roleDSNs {
		source, err := pgxpool.New(ctx, sourceDSN)
		if err != nil {
			return fmt.Errorf("open role shard %d: %w", sourceIndex, err)
		}
		if err := syncRoleShard(ctx, central, source, shardCount); err != nil {
			source.Close()
			return fmt.Errorf("role shard %d: %w", sourceIndex, err)
		}
		source.Close()
	}
	return nil
}

func syncRoleShard(
	ctx context.Context, central, source *pgxpool.Pool, shardCount int,
) error {
	rows, err := source.Query(ctx,
		`SELECT id, account_id, zone_id, name, level, created_at, updated_at
		   FROM roles ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	const batchSize = 500
	var pending []directoryRole
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		var batch pgx.Batch
		for _, role := range pending {
			batch.Queue(upsertDirectoryRoleSQL,
				role.id, role.accountID, role.zoneID, role.name, role.level,
				roleShard(role.id, shardCount), role.createdAt, role.updatedAt)
		}
		results := central.SendBatch(ctx, &batch)
		for range pending {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return err
			}
		}
		if err := results.Close(); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	for rows.Next() {
		var role directoryRole
		if err := rows.Scan(
			&role.id, &role.accountID, &role.zoneID, &role.name, &role.level,
			&role.createdAt, &role.updatedAt,
		); err != nil {
			return err
		}
		pending = append(pending, role)
		if len(pending) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return flush()
}

func roleShard(roleID int64, shardCount int) int {
	shardID := roleID % int64(shardCount)
	if shardID < 0 {
		shardID += int64(shardCount)
	}
	return int(shardID)
}

func run(ctx context.Context, dsn, dir string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7152026)`); err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(7152026)`)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(128) PRIMARY KEY,
			checksum VARCHAR(64) NOT NULL DEFAULT '',
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		ALTER TABLE schema_migrations
			ADD COLUMN IF NOT EXISTS checksum VARCHAR(64) NOT NULL DEFAULT ''
	`); err != nil {
		return err
	}
	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}
	for _, name := range files {
		sqlBytes, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		checksum := fmt.Sprintf("%x", sha256.Sum256(sqlBytes))
		var appliedChecksum string
		err = conn.QueryRow(ctx,
			`SELECT checksum FROM schema_migrations WHERE version = $1`, name,
		).Scan(&appliedChecksum)
		switch {
		case err == nil:
			if appliedChecksum == "" {
				if _, err := conn.Exec(ctx,
					`UPDATE schema_migrations SET checksum = $2 WHERE version = $1`, name, checksum,
				); err != nil {
					return err
				}
			} else if appliedChecksum != checksum {
				return fmt.Errorf("migration %s was modified after being applied", name)
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations(version, checksum) VALUES ($1, $2)`, name, checksum,
		); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		fmt.Printf("applied %s\n", name)
	}
	return nil
}
