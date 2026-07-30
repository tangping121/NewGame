// Package internal 战斗房间服务：创建房间、收集结算结果。
package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	redisx "newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type room struct {
	ID      string                            `json:"id"`
	Members []int64                           `json:"members"`
	Results map[int64]*pb.BattleResultRequest `json:"-"`
	Created time.Time                         `json:"created_at"`
}

type Server struct {
	cfg   config.Service
	log   *zap.Logger
	redis goredis.UniversalClient
	mu    sync.Mutex
	rooms map[string]*room
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	var rdb goredis.UniversalClient
	if cfg.Infra.Redis != "" || len(cfg.Infra.RedisCluster) > 0 {
		rdb = redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		if rdb != nil {
			_ = rdb.Close()
		}
		return nil, err
	}
	s := &Server{
		cfg:   cfg,
		log:   log.New(cfg.LogLevel),
		redis: rdb,
		rooms: make(map[string]*room),
	}
	return s, nil
}

// roomTTL 战斗房间保留时长，超时由后台清理，防止内存无限增长。
const roomTTL = 30 * time.Minute

// cleanupRooms 后台定期清理过期房间。
func (s *Server) cleanupRooms(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-roomTTL)
			s.mu.Lock()
			for id, rm := range s.rooms {
				if rm.Created.Before(cutoff) {
					delete(s.rooms, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/battle/room/create", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleCreate))
	mux.HandleFunc("/api/battle/room", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleGet))
	mux.HandleFunc("/api/battle/settle", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleSettle))
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
	roomID, err := newRoomID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rm := &room{
		ID:      roomID,
		Members: append([]int64(nil), req.Members...),
		Results: make(map[int64]*pb.BattleResultRequest),
		Created: time.Now(),
	}
	if s.redis != nil {
		raw, _ := json.Marshal(rm)
		if err := s.redis.Set(r.Context(), battleRoomKey(roomID), raw, roomTTL).Err(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	} else {
		s.mu.Lock()
		s.rooms[roomID] = rm
		s.mu.Unlock()
	}
	s.log.Info("battle room created", zap.String("room_id", roomID), zap.Int64s("members", req.Members))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "room_id": roomID})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room_id")
	rm, err := s.getRoom(r.Context(), roomID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "room not found"})
		return
	}
	var members []int64
	results := make(map[int64]*pb.BattleResultRequest)
	if rm != nil {
		members = append([]int64(nil), rm.Members...)
		if s.redis != nil {
			values, _ := s.redis.HGetAll(r.Context(), battleResultsKey(roomID)).Result()
			for role, raw := range values {
				var result pb.BattleResultRequest
				if json.Unmarshal([]byte(raw), &result) == nil {
					results[result.RoleId] = &result
				}
				_ = role
			}
		} else {
			s.mu.Lock()
			for roleID, result := range rm.Results {
				results[roleID] = proto.Clone(result).(*pb.BattleResultRequest)
			}
			s.mu.Unlock()
		}
	}
	if rm == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "room not found"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "room_id": roomID, "members": members, "results": results,
	})
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	req := new(pb.BattleResultRequest)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1001})
		return
	}
	rm, err := s.getRoom(r.Context(), req.RoomId)
	if err != nil {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1003})
		return
	}
	isMember := rm != nil && containsRole(rm.Members, req.RoleId)
	var resultCount int64
	if isMember {
		if s.redis != nil {
			raw, _ := json.Marshal(req)
			pipe := s.redis.Pipeline()
			pipe.HSet(r.Context(), battleResultsKey(req.RoomId), req.RoleId, raw)
			pipe.Expire(r.Context(), battleResultsKey(req.RoomId), roomTTL)
			if _, err := pipe.Exec(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		} else {
			s.mu.Lock()
			rm.Results[req.RoleId] = req
			resultCount = int64(len(rm.Results))
			s.mu.Unlock()
		}
	}
	if s.redis != nil {
		resultCount, _ = s.redis.HLen(r.Context(), battleResultsKey(req.RoomId)).Result()
	}
	done := rm != nil && resultCount >= int64(len(rm.Members))
	if rm == nil {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1003})
		return
	}
	if !isMember {
		_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 1002})
		return
	}
	if done {
		s.log.Info("battle room settled", zap.String("room_id", req.RoomId))
	}
	_ = json.NewEncoder(w).Encode(&pb.BattleResultResponse{Code: 0})
}

func battleRoomKey(roomID string) string {
	return fmt.Sprintf("ng:battle:{%s}:room", roomID)
}

func battleResultsKey(roomID string) string {
	return fmt.Sprintf("ng:battle:{%s}:results", roomID)
}

func newRoomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "br_" + hex.EncodeToString(raw[:]), nil
}

func (s *Server) getRoom(ctx context.Context, roomID string) (*room, error) {
	if s.redis != nil {
		raw, err := s.redis.Get(ctx, battleRoomKey(roomID)).Bytes()
		if err != nil {
			return nil, err
		}
		var rm room
		if err := json.Unmarshal(raw, &rm); err != nil {
			return nil, err
		}
		rm.Results = make(map[int64]*pb.BattleResultRequest)
		return &rm, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rm := s.rooms[roomID]
	if rm == nil {
		return nil, fmt.Errorf("room not found")
	}
	return rm, nil
}

func containsRole(members []int64, roleID int64) bool {
	for _, member := range members {
		if member == roleID {
			return true
		}
	}
	return false
}

func (s *Server) Run() error {
	s.log.Info("battle service ready", zap.String("addr", s.cfg.HTTPAddr))
	closers := make([]app.CloseFunc, 0, 1)
	if s.redis != nil {
		closers = append(closers, app.CloseNoContext(s.redis.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		go s.cleanupRooms(ctx)
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
