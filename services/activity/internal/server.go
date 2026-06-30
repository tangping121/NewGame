// Package internal 活动服务：活动进度读写（Postgres 或内存）。
package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/repo"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

const login7DayActivity = 1001

// maxMemProgressRoles 无 Postgres 时内存进度表最多保留角色数，超出则驱逐一条旧记录。
const maxMemProgressRoles = 10000

type Server struct {
	cfg        config.Service
	log        *zap.Logger
	activities *repo.ActivityRepo
	progMu     sync.RWMutex                            // 保护 progress（无 DB 时）
	progress   map[int64]map[int32]repo.ActivityState // 仅 activities==nil 时使用
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	s := &Server{cfg: cfg, log: logger, progress: map[int64]map[int32]repo.ActivityState{}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.activities = repo.NewActivityRepo(pool)
		} else {
			logger.Warn("postgres connect failed", zap.Error(err))
		}
	}
	if cfg.Infra.NATS != "" {
		if nc, err := mq.Connect(cfg.Infra.NATS); err == nil {
			_, _ = nc.Subscribe(mq.SubjectActivityEvt, func(m *nats.Msg) {
				var evt struct {
					RoleId     int64 `json:"role_id"`
					ActivityId int32 `json:"activity_id"`
					Delta      int32 `json:"delta"`
				}
				if json.Unmarshal(m.Data, &evt) == nil {
					if _, err := s.addProgress(context.Background(), evt.RoleId, evt.ActivityId, evt.Delta); err != nil {
						s.log.Warn("activity nats progress failed", zap.Error(err))
					}
				}
			})
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
	mux.HandleFunc("/api/activity/list", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"activities": map[int32]map[string]any{
				login7DayActivity: {"name": "login_7day", "status": "open", "target": 7},
			},
		})
	})
	mux.HandleFunc("/api/activity/progress", s.handleProgress)
	mux.HandleFunc("/api/activity/claim", s.handleClaim)
	return mux
}

func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	roleID, _ := strconv.ParseInt(r.URL.Query().Get("role_id"), 10, 64)
	activityID, _ := strconv.ParseInt(r.URL.Query().Get("activity_id"), 10, 32)
	st, _ := s.getState(r.Context(), roleID, int32(activityID))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "progress": st.Progress, "claimed": st.Claimed})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId     int64 `json:"role_id"`
		ActivityId int32 `json:"activity_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	st, err := s.claim(r.Context(), req.RoleId, req.ActivityId, 7)
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
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
