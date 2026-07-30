// Package internal 活动服务：活动进度读写（Postgres 或内存）。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"
	"newgame/pkg/session"

	"github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const login7DayActivity = 1001

// maxMemProgressRoles 无 Postgres 时内存进度表最多保留角色数，超出则驱逐一条旧记录。
const maxMemProgressRoles = 10000

type Server struct {
	cfg        config.Service
	log        *zap.Logger
	redis      goredis.UniversalClient
	nats       *nats.Conn
	game       *client.GameClient
	activities *repo.ActivityRepo
	progMu     sync.RWMutex                           // 保护 progress（无 DB 时）
	progress   map[int64]map[int32]repo.ActivityState // 仅 activities==nil 时使用
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	disc := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	s := &Server{
		cfg: cfg, log: logger, redis: rdb,
		game:     client.NewGameClientSharded(disc, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret).WithStrict(cfg.Production()),
		progress: map[int64]map[int32]repo.ActivityState{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.activities = repo.NewActivityRepo(pool)
		} else {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
	}
	if cfg.Infra.NATS != "" {
		if nc, err := mq.Connect(cfg.Infra.NATS); err == nil {
			s.nats = nc
			if _, err := mq.SubscribeDurable(nc, mq.SubjectActivityEvt, "activity-v1", func(ctx context.Context, event mq.Event) error {
				var evt struct {
					RoleId     int64 `json:"role_id"`
					ActivityId int32 `json:"activity_id"`
					Delta      int32 `json:"delta"`
				}
				if err := json.Unmarshal(event.Data, &evt); err != nil {
					return err
				}
				if s.activities != nil {
					_, _, err := s.activities.AddProgressEvent(ctx, event.EventID, evt.RoleId, evt.ActivityId, evt.Delta)
					return err
				}
				_, err := s.addProgress(ctx, evt.RoleId, evt.ActivityId, evt.Delta)
				return err
			}); err != nil {
				nc.Close()
				return nil, fmt.Errorf("subscribe activity stream: %w", err)
			}
		} else if cfg.Production() {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
	}
	return s, nil
}

func (s *Server) addProgress(ctx context.Context, roleID int64, activityID, delta int32) (repo.ActivityState, error) {
	if s.activities != nil {
		return s.activities.AddProgress(ctx, roleID, activityID, delta)
	}
	s.progMu.Lock()
	defer s.progMu.Unlock()
	if s.progress[roleID] == nil {
		if len(s.progress) >= maxMemProgressRoles {
			for k := range s.progress {
				delete(s.progress, k)
				break
			}
		}
		s.progress[roleID] = map[int32]repo.ActivityState{}
	}
	st := s.progress[roleID][activityID]
	st.ActivityID = activityID
	st.Progress += delta
	s.progress[roleID][activityID] = st
	return st, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	auth := func(h http.HandlerFunc) http.HandlerFunc { return session.HTTPMiddleware(s.redis, h) }
	mux.HandleFunc("/api/activity/list", auth(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"activities": map[int32]map[string]any{
				login7DayActivity: {"name": "login_7day", "status": "open", "target": 7},
			},
		})
	}))
	mux.HandleFunc("/api/activity/progress", auth(s.handleProgress))
	mux.HandleFunc("/api/activity/claim", auth(s.handleClaim))
	return mux
}

func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	roleID := session.RoleID(r.Context())
	activityID, _ := strconv.ParseInt(r.URL.Query().Get("activity_id"), 10, 32)
	st, _ := s.getState(r.Context(), roleID, int32(activityID))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "progress": st.Progress, "claimed": st.Claimed})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ActivityId int32 `json:"activity_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ActivityId <= 0 {
		http.Error(w, "activity_id required", http.StatusBadRequest)
		return
	}
	roleID := session.RoleID(r.Context())
	current, err := s.getState(r.Context(), roleID, req.ActivityId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !current.Claimed && current.Progress >= 7 {
		if err := s.game.GrantItems(r.Context(), roleID, "gold:100", fmt.Sprintf("activity:%d:%d", roleID, req.ActivityId)); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	st, err := s.claim(r.Context(), roleID, req.ActivityId, 7)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rewards := []string{}
	if st.Claimed {
		rewards = []string{"gold:100"}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "claimed": st.Claimed, "rewards": rewards})
}

func (s *Server) getState(ctx context.Context, roleID int64, activityID int32) (repo.ActivityState, error) {
	if s.activities != nil {
		return s.activities.Get(ctx, roleID, activityID)
	}
	s.progMu.RLock()
	defer s.progMu.RUnlock()
	if s.progress[roleID] == nil {
		return repo.ActivityState{ActivityID: activityID}, nil
	}
	return s.progress[roleID][activityID], nil
}

func (s *Server) claim(ctx context.Context, roleID int64, activityID, need int32) (repo.ActivityState, error) {
	if s.activities != nil {
		return s.activities.Claim(ctx, roleID, activityID, need)
	}
	s.progMu.Lock()
	defer s.progMu.Unlock()
	st := repo.ActivityState{ActivityID: activityID}
	if s.progress[roleID] != nil {
		st = s.progress[roleID][activityID]
	}
	if st.Progress >= need && !st.Claimed {
		st.Claimed = true
		if s.progress[roleID] == nil {
			s.progress[roleID] = map[int32]repo.ActivityState{}
		}
		s.progress[roleID][activityID] = st
	}
	return st, nil
}

func (s *Server) Run() error {
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.nats != nil {
		closers = append(closers, app.CloseNoContext(s.nats.Drain))
	}
	if s.activities != nil {
		closers = append(closers, app.CloseVoid(s.activities.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
