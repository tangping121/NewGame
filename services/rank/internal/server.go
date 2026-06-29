// Package internal 排行榜服务：单区/全服 Redis ZSET，订阅 NATS 异步更新。
package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/rankkey"
	redisx "newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Server struct {
	cfg   config.Service
	log   *zap.Logger
	redis goredis.UniversalClient
	keys  sync.Map // 已写入的榜 key -> struct{}，供后台裁剪
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	s := &Server{cfg: cfg, log: logger, redis: rdb}
	if cfg.Infra.NATS != "" {
		nc, err := mq.Connect(cfg.Infra.NATS)
		if err == nil {
			_, _ = nc.Subscribe(mq.SubjectRankUpdate, func(m *nats.Msg) {
				var req pb.RankUpdateRequest
				if json.Unmarshal(m.Data, &req) == nil {
					s.writeScore(context.Background(), &req)
				}
			})
		}
	}
	go s.trimLoop()
	return s, nil
}

// writeScore 写入单区榜与全服榜（不再写 legacy 旧键，避免三倍写放大）。
func (s *Server) writeScore(ctx context.Context, req *pb.RankUpdateRequest) {
	member := strconv.FormatInt(req.RoleId, 10)
	if req.ZoneId > 0 {
		member = rankkey.Member(req.ZoneId, req.RoleId)
	}
	score := goredis.Z{Score: float64(req.Score), Member: member}
	zone := req.ZoneId
	if zone == 0 {
		zone = s.cfg.ZoneID
	}
	zoneKey := rankkey.Zone(zone, req.Board)
	globalKey := rankkey.Global(req.Board)
	_ = s.redis.ZAdd(ctx, zoneKey, score).Err()
	_ = s.redis.ZAdd(ctx, globalKey, score).Err()
	s.keys.Store(zoneKey, struct{}{})
	s.keys.Store(globalKey, struct{}{})
}

// trimLoop 后台定期将各榜裁剪到 TopN，防止 ZSET 无限增长成为热/大 key。
func (s *Server) trimLoop() {
	cap := s.cfg.Rank.RankCap()
	ticker := time.NewTicker(s.cfg.Rank.TrimInterval())
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.keys.Range(func(k, _ any) bool {
			key := k.(string)
			// 保留分数最高的 cap 个：删除排名 [0, len-cap) 的低分成员。
			if err := s.redis.ZRemRangeByRank(ctx, key, 0, int64(-cap-1)).Err(); err != nil {
				s.log.Warn("rank trim failed", zap.String("key", key), zap.Error(err))
			}
			return true
		})
		cancel()
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/rank/update", s.handleUpdate)
	mux.HandleFunc("/api/rank/top", s.handleTop)
	mux.HandleFunc("/api/rank/global/top", s.handleGlobalTop)
	return mux
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req pb.RankUpdateRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.ZoneId == 0 {
		req.ZoneId = s.cfg.ZoneID
	}
	s.writeScore(r.Context(), &req)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleTop(w http.ResponseWriter, r *http.Request) {
	board := r.URL.Query().Get("board")
	if board == "" {
		board = "dungeon"
	}
	zoneID, _ := strconv.ParseInt(r.URL.Query().Get("zone_id"), 10, 32)
	if zoneID == 0 {
		zoneID = int64(s.cfg.ZoneID)
	}
	vals, _ := s.redis.ZRevRangeWithScores(r.Context(), rankkey.Zone(int32(zoneID), board), 0, 9).Result()
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "zone_id": zoneID, "list": vals})
}

func (s *Server) handleGlobalTop(w http.ResponseWriter, r *http.Request) {
	board := r.URL.Query().Get("board")
	if board == "" {
		board = "dungeon"
	}
	vals, _ := s.redis.ZRevRangeWithScores(r.Context(), rankkey.Global(board), 0, 19).Result()
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "list": vals})
}

func (s *Server) Run() error {
	s.log.Info("rank service ready", zap.String("addr", s.cfg.HTTPAddr))
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
