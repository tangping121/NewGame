package repo

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ActivityState struct {
	ActivityID int32
	Progress   int32
	Claimed    bool
}

type ActivityRepo struct {
	pool *pgxpool.Pool
}

func NewActivityRepo(pool *pgxpool.Pool) *ActivityRepo {
	return &ActivityRepo{pool: pool}
}

func (r *ActivityRepo) Get(ctx context.Context, roleID int64, activityID int32) (ActivityState, error) {
	var s ActivityState
	s.ActivityID = activityID
	err := r.pool.QueryRow(ctx,
		`SELECT progress, claimed FROM activity_progress WHERE role_id = $1 AND activity_id = $2`,
		roleID, activityID,
	).Scan(&s.Progress, &s.Claimed)
	if err != nil {
		return ActivityState{ActivityID: activityID}, nil
	}
	return s, nil
}

func (r *ActivityRepo) AddProgress(ctx context.Context, roleID int64, activityID, delta int32) (ActivityState, error) {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO activity_progress (role_id, activity_id, progress)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (role_id, activity_id)
		 DO UPDATE SET progress = activity_progress.progress + EXCLUDED.progress, updated_at = NOW()`,
		roleID, activityID, delta,
	)
	if err != nil {
		return ActivityState{}, err
	}
	return r.Get(ctx, roleID, activityID)
}

func (r *ActivityRepo) Claim(ctx context.Context, roleID int64, activityID, need int32) (ActivityState, error) {
	s, err := r.Get(ctx, roleID, activityID)
	if err != nil {
		return s, err
	}
	if s.Claimed || s.Progress < need {
		return s, nil
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO activity_progress (role_id, activity_id, progress, claimed)
		 VALUES ($1, $2, $3, true)
		 ON CONFLICT (role_id, activity_id) DO UPDATE SET claimed = true, updated_at = NOW()`,
		roleID, activityID, s.Progress,
	)
	if err != nil {
		return s, err
	}
	s.Claimed = true
	return s, nil
}
