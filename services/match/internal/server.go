// Package internal 匹配服务：单区内存队列或 Redis 跨服队列，凑满后创建 Battle 房间。
package internal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/discovery"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	redisx "newgame/pkg/redis"
	"newgame/pkg/session"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const playersPerMatch = 2 // 匹配成功所需人数

var battleHTTPClient = &http.Client{Timeout: 5 * time.Second}

// roomEntry 匹配生成的房间，带创建时间用于 TTL 清理。
type roomEntry struct {
	Members []int64   `json:"members"`
	RoomID  string    `json:"room_id,omitempty"`
	State   string    `json:"state"`
	Created time.Time `json:"created_at"`
}

// roomTTL 房间记录保留时长，超时由后台清理，防止内存无限增长。
const roomTTL = 10 * time.Minute

type Server struct {
	cfg       config.Service
	log       *zap.Logger
	resolver  *discovery.Resolver // Battle 实例解析 TTL 缓存
	redis     goredis.UniversalClient
	crossPool *CrossPool
	poolsMu   sync.Mutex // 保护 pools（本地等待队列）
	pools     map[int32][]int64
	roomsMu   sync.Mutex // 保护 rooms
	rooms     map[string]*roomEntry
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	var resolver *discovery.Resolver
	var rdb goredis.UniversalClient
	if cfg.Infra.Redis != "" || len(cfg.Infra.RedisCluster) > 0 {
		rdb = redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
		resolver = discovery.NewResolver(discovery.NewRegistry(rdb, cfg.Discovery.TTL()), 2*time.Second)
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
		cfg:       cfg,
		log:       log.New(cfg.LogLevel),
		resolver:  resolver,
		redis:     rdb,
		crossPool: NewCrossPool(rdb),
		pools:     make(map[int32][]int64),
		rooms:     make(map[string]*roomEntry),
	}
	if cfg.CrossZoneMatch && rdb == nil {
		return nil, fmt.Errorf("cross_zone_match requires redis")
	}
	return s, nil
}

// cleanupRooms 后台定期清理超过 roomTTL 的房间记录。
func (s *Server) cleanupRooms(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-roomTTL)
			s.roomsMu.Lock()
			for id, rm := range s.rooms {
				if rm.Created.Before(cutoff) {
					delete(s.rooms, id)
				}
			}
			s.roomsMu.Unlock()
		}
	}
}

// putRoom 记录房间并打时间戳。
func (s *Server) putRoom(ctx context.Context, matchID string, members []int64, state, battleRoomID string) error {
	entry := &roomEntry{Members: append([]int64(nil), members...), RoomID: battleRoomID, State: state, Created: time.Now()}
	if s.redis != nil {
		raw, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		pipe := s.redis.Pipeline()
		pipe.Set(ctx, "ng:match:room:"+matchID, raw, roomTTL)
		for _, roleID := range members {
			pipe.Set(ctx, fmt.Sprintf("ng:match:ticket:%d", roleID), matchID, roomTTL)
		}
		_, err = pipe.Exec(ctx)
		return err
	}
	s.roomsMu.Lock()
	s.rooms[matchID] = entry
	s.roomsMu.Unlock()
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	auth := func(h http.HandlerFunc) http.HandlerFunc { return session.HTTPMiddleware(s.redis, h) }
	mux.HandleFunc("/api/match/join", auth(s.handleJoin))
	mux.HandleFunc("/api/match/room", auth(s.handleRoom))
	mux.HandleFunc("/api/match/queue", auth(s.handleQueue))
	return mux
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req pb.MatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 1001, MatchId: err.Error()})
		return
	}
	info, _ := session.FromContext(r.Context())
	req.RoleId = info.RoleID
	req.ZoneId = info.ZoneID
	if req.ZoneId <= 0 {
		req.ZoneId = s.cfg.ZoneID
	}

	if s.cfg.CrossZoneMatch {
		s.handleCrossJoin(w, r, &req, "global")
		return
	}
	s.handleLocalJoin(w, r, &req)
}

func (s *Server) handleCrossJoin(
	w http.ResponseWriter, r *http.Request, req *pb.MatchRequest, scope string,
) {
	matched, err := s.crossPool.Join(r.Context(), scope, req.Mode, req.ZoneId, req.RoleId, playersPerMatch)
	if err != nil {
		s.log.Warn("cross match join failed", zap.Error(err))
		_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
		return
	}
	resp := pb.MatchResponse{Code: 0, MatchId: fmt.Sprintf("wait_%d", req.RoleId)}
	if len(matched) >= playersPerMatch {
		roleIDs := make([]int64, len(matched))
		for i, e := range matched {
			roleIDs[i] = e.RoleID
		}
		matchID, err := newMatchID("cx")
		if err != nil {
			_ = s.crossPool.Requeue(r.Context(), scope, req.Mode, matched)
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		if err := s.putRoom(r.Context(), matchID, roleIDs, "RESERVED", ""); err != nil {
			_ = s.crossPool.Requeue(r.Context(), scope, req.Mode, matched)
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		roomID, err := s.createBattleRoom(r.Context(), roleIDs)
		if err != nil {
			s.deleteRoom(r.Context(), matchID, roleIDs)
			_ = s.crossPool.Requeue(r.Context(), scope, req.Mode, matched)
			s.log.Warn("battle room create failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		if err := s.putRoom(r.Context(), matchID, roleIDs, "ROOM_CREATED", roomID); err != nil {
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		resp.MatchId = matchID
		resp.RoomId = roomID
		s.log.Info("cross-zone match",
			zap.String("match_id", matchID),
			zap.String("room_id", roomID),
			zap.Int64s("roles", roleIDs),
		)
	}
	_ = json.NewEncoder(w).Encode(&resp)
}

func (s *Server) handleLocalJoin(w http.ResponseWriter, r *http.Request, req *pb.MatchRequest) {
	if s.crossPool != nil && s.redis != nil {
		s.handleCrossJoin(w, r, req, fmt.Sprintf("zone:%d", req.ZoneId))
		return
	}
	s.poolsMu.Lock()
	pool := s.pools[req.Mode]
	pool = appendUnique(pool, req.RoleId)
	resp := pb.MatchResponse{Code: 0}

	var members []int64
	if len(pool) >= playersPerMatch {
		members = append([]int64(nil), pool[:playersPerMatch]...)
		s.pools[req.Mode] = pool[playersPerMatch:]
	} else {
		s.pools[req.Mode] = pool
		resp.MatchId = fmt.Sprintf("wait_%d", req.RoleId)
	}
	s.poolsMu.Unlock()

	// 创建战斗房间走网络调用，放到锁外，避免阻塞其他匹配请求。
	if members != nil {
		matchID, idErr := newMatchID("m")
		if idErr != nil {
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: idErr.Error()})
			return
		}
		_ = s.putRoom(r.Context(), matchID, members, "RESERVED", "")
		roomID, err := s.createBattleRoom(r.Context(), members)
		if err != nil {
			s.log.Warn("battle room create failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		if err := s.putRoom(r.Context(), matchID, members, "ROOM_CREATED", roomID); err != nil {
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		resp.MatchId = matchID
		resp.RoomId = roomID
	}

	_ = json.NewEncoder(w).Encode(&resp)
}

func appendUnique(pool []int64, id int64) []int64 {
	for _, v := range pool {
		if v == id {
			return pool
		}
	}
	return append(pool, id)
}

func (s *Server) createBattleRoom(ctx context.Context, members []int64) (string, error) {
	base := s.battleURL(ctx)
	if base == "" {
		return "", fmt.Errorf("battle service unavailable")
	}
	body, _ := json.Marshal(map[string]any{"members": members})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/battle/room/create", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	internalauth.SetHTTP(req, s.cfg.InternalSecret)
	resp, err := battleHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("battle returned %s: %s", resp.Status, string(responseBody))
	}
	var out struct {
		Code   int32  `json:"code"`
		RoomID string `json:"room_id"`
	}
	if err := json.Unmarshal(responseBody, &out); err != nil {
		return "", err
	}
	if out.Code != 0 || out.RoomID == "" {
		return "", fmt.Errorf("battle rejected room creation: %s", string(responseBody))
	}
	return out.RoomID, nil
}

// battleURL 解析 Battle 服务 HTTP 基址。
//
// 优先 ResolveGlobal（跨区 Battle）；失败再 Resolve 本区；均无则本地回退。
func (s *Server) battleURL(ctx context.Context) string {
	if s.resolver != nil && s.cfg.Discovery.Enabled {
		if inst, ok := s.resolver.ResolveGlobal(ctx, "battle"); ok {
			return inst.HTTPBase()
		}
		if inst, ok := s.resolver.Resolve(ctx, "battle", s.cfg.ZoneID); ok {
			return inst.HTTPBase()
		}
		s.log.Warn("battle discovery failed", zap.Int32("zone", s.cfg.ZoneID))
	}
	if s.cfg.Production() {
		return ""
	}
	return "http://127.0.0.1:9300"
}

func (s *Server) handleRoom(w http.ResponseWriter, r *http.Request) {
	matchID := r.URL.Query().Get("match_id")
	roleID := session.RoleID(r.Context())
	if strings.HasPrefix(matchID, "wait_") && s.redis != nil {
		waitingRoleID, _ := strconv.ParseInt(strings.TrimPrefix(matchID, "wait_"), 10, 64)
		if waitingRoleID != roleID {
			http.Error(w, "match ticket does not belong to session", http.StatusForbidden)
			return
		}
		if actual, err := s.redis.Get(r.Context(), fmt.Sprintf("ng:match:ticket:%d", roleID)).Result(); err == nil {
			matchID = actual
		}
	}
	rm, err := s.getRoom(r.Context(), matchID)
	if err != nil || rm == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "match not found"})
		return
	}
	if !roomHasRole(rm, roleID) {
		http.Error(w, "match does not belong to session", http.StatusForbidden)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "match_id": matchID, "members": rm.Members,
		"state":   valueOrEmpty(rm, func(v *roomEntry) string { return v.State }),
		"room_id": valueOrEmpty(rm, func(v *roomEntry) string { return v.RoomID }),
	})
}

func roomHasRole(room *roomEntry, roleID int64) bool {
	for _, member := range room.Members {
		if member == roleID {
			return true
		}
	}
	return false
}

func (s *Server) getRoom(ctx context.Context, matchID string) (*roomEntry, error) {
	if s.redis != nil {
		raw, err := s.redis.Get(ctx, "ng:match:room:"+matchID).Bytes()
		if err != nil {
			return nil, err
		}
		var entry roomEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}
		return &entry, nil
	}
	s.roomsMu.Lock()
	defer s.roomsMu.Unlock()
	return s.rooms[matchID], nil
}

func (s *Server) deleteRoom(ctx context.Context, matchID string, members []int64) {
	if s.redis != nil {
		pipe := s.redis.Pipeline()
		pipe.Del(ctx, "ng:match:room:"+matchID)
		for _, roleID := range members {
			pipe.Del(ctx, fmt.Sprintf("ng:match:ticket:%d", roleID))
		}
		_, _ = pipe.Exec(ctx)
		return
	}
	s.roomsMu.Lock()
	delete(s.rooms, matchID)
	s.roomsMu.Unlock()
}

func newMatchID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}

func valueOrEmpty[T any](value *roomEntry, selectValue func(*roomEntry) T) T {
	if value == nil {
		var zero T
		return zero
	}
	return selectValue(value)
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	mode := int32(0)
	if s.crossPool != nil && s.redis != nil {
		scope := "global"
		if !s.cfg.CrossZoneMatch {
			info, _ := session.FromContext(r.Context())
			scope = fmt.Sprintf("zone:%d", info.ZoneID)
		}
		n, err := s.crossPool.QueueSize(r.Context(), scope, mode)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "cross_zone": true, "waiting": n})
		return
	}
	s.poolsMu.Lock()
	n := len(s.pools[mode])
	s.poolsMu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "cross_zone": false, "waiting": n})
}

func (s *Server) Run() error {
	s.log.Info("match service ready",
		zap.String("addr", s.cfg.HTTPAddr),
		zap.Bool("cross_zone", s.cfg.CrossZoneMatch),
	)
	closers := make([]app.CloseFunc, 0, 1)
	if s.redis != nil {
		closers = append(closers, app.CloseNoContext(s.redis.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		go s.cleanupRooms(ctx)
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
