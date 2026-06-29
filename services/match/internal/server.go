// Package internal 匹配服务：单区内存队列或 Redis 跨服队列，凑满后创建 Battle 房间。
package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/discovery"
	"newgame/pkg/log"
	redisx "newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const playersPerMatch = 2 // 匹配成功所需人数

// roomEntry 匹配生成的房间，带创建时间用于 TTL 清理。
type roomEntry struct {
	members []int64
	created time.Time
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
	if cfg.Infra.Redis != "" {
		rdb = redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
		resolver = discovery.NewResolver(discovery.NewRegistry(rdb, cfg.Discovery.TTL()), 2*time.Second)
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
	go s.cleanupRooms()
	return s, nil
}

// cleanupRooms 后台定期清理超过 roomTTL 的房间记录。
func (s *Server) cleanupRooms() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-roomTTL)
		s.roomsMu.Lock()
		for id, rm := range s.rooms {
			if rm.created.Before(cutoff) {
				delete(s.rooms, id)
			}
		}
		s.roomsMu.Unlock()
	}
}

// putRoom 记录房间并打时间戳。
func (s *Server) putRoom(matchID string, members []int64) {
	s.roomsMu.Lock()
	s.rooms[matchID] = &roomEntry{members: members, created: time.Now()}
	s.roomsMu.Unlock()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/match/join", s.handleJoin)
	mux.HandleFunc("/api/match/room", s.handleRoom)
	mux.HandleFunc("/api/match/queue", s.handleQueue)
	return mux
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req pb.MatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 1001, MatchId: err.Error()})
		return
	}
	if req.RoleId == 0 {
		_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 1001, MatchId: "role_id required"})
		return
	}
	if req.ZoneId == 0 {
		req.ZoneId = s.cfg.ZoneID
	}

	if s.cfg.CrossZoneMatch {
		s.handleCrossJoin(w, r, &req)
		return
	}
	s.handleLocalJoin(w, r, &req)
}

func (s *Server) handleCrossJoin(w http.ResponseWriter, r *http.Request, req *pb.MatchRequest) {
	matched, err := s.crossPool.Join(r.Context(), req.Mode, req.ZoneId, req.RoleId, playersPerMatch)
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
		roomID, err := s.createBattleRoom(r.Context(), roleIDs)
		if err != nil {
			s.log.Warn("battle room create failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		matchID := fmt.Sprintf("cx_%d_%d", roleIDs[0], roleIDs[1])
		s.putRoom(matchID, roleIDs)
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
		roomID, err := s.createBattleRoom(r.Context(), members)
		if err != nil {
			s.log.Warn("battle room create failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(pb.MatchResponse{Code: 5000, MatchId: err.Error()})
			return
		}
		matchID := fmt.Sprintf("m_%d_%d", members[0], members[1])
		s.putRoom(matchID, members)
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
	body, _ := json.Marshal(map[string]any{"members": members})
	resp, err := http.Post(base+"/api/battle/room/create", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Code   int32  `json:"code"`
		RoomID string `json:"room_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.RoomID == "" {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("empty room_id: %s", string(b))
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
	return "http://127.0.0.1:9300"
}

func (s *Server) handleRoom(w http.ResponseWriter, r *http.Request) {
	matchID := r.URL.Query().Get("match_id")
	s.roomsMu.Lock()
	rm := s.rooms[matchID]
	s.roomsMu.Unlock()
	members := []int64{}
	if rm != nil {
		members = rm.members
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "members": members})
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	mode := int32(0)
	if s.cfg.CrossZoneMatch && s.crossPool != nil {
		n, err := s.crossPool.QueueSize(r.Context(), mode)
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
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
