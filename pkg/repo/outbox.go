package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxEvent is committed beside an aggregate snapshot and later published
// to JetStream by a retrying worker.
type OutboxEvent struct {
	ID               int64
	PoolIndex        int
	EventID          string
	Subject          string
	AggregateID      string
	AggregateVersion int64
	Payload          json.RawMessage
}

// SaveWithOutbox atomically advances a role snapshot and inserts an event.
func (r *RoleRepo) SaveWithOutbox(
	ctx context.Context, roleID int64, snap RoleSnapshot, event OutboxEvent,
) (int64, error) {
	if event.EventID == "" || event.Subject == "" {
		return 0, fmt.Errorf("outbox event id and subject are required")
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, err
	}
	tx, err := r.poolFor(roleID).Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var version int64
	err = tx.QueryRow(ctx,
		`UPDATE roles
		    SET level = $2, snapshot = $3, version = version + 1, updated_at = NOW()
		  WHERE id = $1 AND version = $4 AND owner_epoch = $5
		  RETURNING version`,
		roleID, snap.Level, raw, snap.Version, snap.OwnerEpoch,
	).Scan(&version)
	if err == pgx.ErrNoRows {
		return 0, ErrStaleRole
	}
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO event_outbox
		    (event_id, subject, aggregate_id, aggregate_ver, payload)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.Subject, event.AggregateID, version, event.Payload,
	)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return version, nil
}

// ReserveOutbox leases pending events using SKIP LOCKED so all Game replicas
// can run publishers without double-claiming work.
func (r *RoleRepo) ReserveOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	var events []OutboxEvent
	perPool := limit
	if len(r.pools) > 1 {
		perPool = (limit + len(r.pools) - 1) / len(r.pools)
	}
	for poolIndex, pool := range r.pools {
		rows, err := pool.Query(ctx, `
			UPDATE event_outbox
			   SET available_at = NOW() + INTERVAL '30 seconds',
			       attempts = attempts + 1
			 WHERE id IN (
			       SELECT id FROM event_outbox
			        WHERE published_at IS NULL AND available_at <= NOW()
			        ORDER BY id
			        FOR UPDATE SKIP LOCKED
			        LIMIT $1
			 )
			 RETURNING id, event_id, subject, aggregate_id, aggregate_ver, payload`,
			perPool,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var event OutboxEvent
			event.PoolIndex = poolIndex
			if err := rows.Scan(
				&event.ID, &event.EventID, &event.Subject, &event.AggregateID,
				&event.AggregateVersion, &event.Payload,
			); err != nil {
				rows.Close()
				return nil, err
			}
			events = append(events, event)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (r *RoleRepo) MarkOutboxPublished(ctx context.Context, poolIndex int, id int64) error {
	if poolIndex < 0 || poolIndex >= len(r.pools) {
		return fmt.Errorf("invalid outbox pool index")
	}
	_, err := r.pools[poolIndex].Exec(ctx,
		`UPDATE event_outbox SET published_at = $2 WHERE id = $1`,
		id, time.Now().UTC(),
	)
	return err
}
