// Package internal 战斗房间服务：创建房间、收集结算结果。
package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/log"

	"go.uber.org/zap"
)

type room struct {
	ID      string
	Members []int64
	Results map[int64]*pb.BattleResultRequest
	Created time.Time
}

type Server struct {
	cfg   config.Service
	log   *zap.Logger
	mu    sync.Mutex
	rooms map[string]*room
	seq   int64
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	return &Server{
		cfg:   cfg,
		log:   log.New(cfg.LogLevel),
		rooms: make(map[string]*room),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/battle/room/create", s.handleCreate)
	mux.HandleFunc("/api/battle/room", s.handleGet)
	mux.HandleFunc("/api/battle/settle", s.handleSettle)
	return mux
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Members []int64 `json:"members"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Members) < 2 {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "need at least 2 members"})
		return
	}
	s.mu.Lock()
	s.seq++
	roomID := fmt.Sprintf("br_%d", s.seq)
	s.rooms[roomID] = &room{
		ID:      roomID,
		Members: append([]int64(nil), req.Members...),
		Results: make(map[int64]*pb.BattleResultRequest),
		Created: time.Now(),
	}
	s.mu.Unlock()
	s.log.Info("battle room created", zap.String("room_id", roomID), zap.Int64s("members", req.Members))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "room_id": roomID})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room_id")
	s.mu.Lock()
	rm := s.rooms[roomID]
	s.mu.Unlock()
	if rm == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "room not found"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "room_id": rm.ID, "members": rm.Members, "results": rm.Results,
	})
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	req := new(pb.BattleResultRequest)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1001})
		return
	}
	s.mu.Lock()
	rm := s.rooms[req.RoomId]
	if rm != nil {
		rm.Results[req.RoleId] = req
	}
	done := rm != nil && len(rm.Results) >= len(rm.Members)
	s.mu.Unlock()
	if rm == nil {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1003})
		return
	}
	if done {
		s.log.Info("battle room settled", zap.String("room_id", req.RoomId))
	}
	_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 0})
}

func (s *Server) Run() error {
	s.log.Info("battle service ready", zap.String("addr", s.cfg.HTTPAddr))
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
