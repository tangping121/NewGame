// Package internal 排行榜服务：单区/全服 Redis ZSET，订阅 NATS 异步更新。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"bastion/api/pb"
	"bastion/pkg/app"
	"bastion/pkg/config"
	"bastion/pkg/internalauth"
	"bastion/pkg/log"
	"bastion/pkg/mq"
	"bastion/pkg/rankkey"
	redisx "bastion/pkg/redis"

	"github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Server struct {
	cfg   config.Service
	log   *zap.Logger
	redis goredis.UniversalClient
	nats  *nats.Conn
	keys  sync.Map // 已写入的榜 key -> struct{}，供后台裁剪
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	s := &Server{cfg: cfg, log: logger, redis: rdb}
	if cfg.Infra.NATS != "" {
		nc, err := mq.Connect(cfg.Infra.NATS)
		if err == nil {
			s.nats = nc
			if _, err := mq.SubscribeDurable(nc, mq.SubjectRankUpdate, "rank-v1", func(ctx context.Context, event mq.Event) error {
				var req pb.RankUpdateRequest
				if err := json.Unmarshal(event.Data, &req); err != nil {
					return err
				}
				return s.writeScore(ctx, &req)
			}); err != nil {
				nc.Close()
				return nil, fmt.Errorf("subscribe rank stream: %w", err)
			}
		} else if cfg.Production() {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
	}
	return s, nil
}

// writeScore 写入单区榜与全服榜（不再写 legacy 旧键，避免三倍写放大）。
func (s *Server) writeScore(ctx context.Context, req *pb.RankUpdateRequest) error {
	member := strconv.FormatInt(req.RoleId, 10)
	if req.ZoneId > 0 {
		member = rankkey.Member(req.ZoneId, req.RoleId)
	}
	score := goredis.Z{Score: float64(req.Score), Member: member}
	update := goredis.ZAddArgs{GT: true, Members: []goredis.Z{score}}
	zone := req.ZoneId
	if zone == 0 {
		zone = s.cfg.ZoneID
	}
	zoneKey := rankkey.Zone(zone, req.Board)
	globalKey := rankkey.Global(req.Board)
	if err := s.redis.ZAddArgs(ctx, zoneKey, update).Err(); err != nil {
		redisx.RecordError("rank", "zadd_zone")
		s.log.Warn("rank zadd zone failed", zap.String("key", zoneKey), zap.Error(err))
		return err
	}
	if err := s.redis.ZAddArgs(ctx, globalKey, update).Err(); err != nil {
		redisx.RecordError("rank", "zadd_global")
		s.log.Warn("rank zadd global failed", zap.String("key", globalKey), zap.Error(err))
		return err
	}
	s.keys.Store(zoneKey, struct{}{})
	s.keys.Store(globalKey, struct{}{})
	return nil
}

// trimLoop 后台定期将各榜裁剪到 TopN，防止 ZSET 无限增长成为热/大 key。
func (s *Server) trimLoop(ctx context.Context) {
	cap := s.cfg.Rank.RankCap()
	ticker := time.NewTicker(s.cfg.Rank.TrimInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/rank/update", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleUpdate))
	mux.HandleFunc("/api/rank/top", s.handleTop)
	mux.HandleFunc("/api/rank/global/top", s.handleGlobalTop)
	return mux
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req pb.RankUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ZoneId == 0 {
		req.ZoneId = s.cfg.ZoneID
	}
	if req.RoleId <= 0 || req.Board == "" {
		http.Error(w, "role_id and board are required", http.StatusBadRequest)
		return
	}
	if err := s.writeScore(r.Context(), &req); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
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
	vals, err := s.redis.ZRevRangeWithScores(r.Context(), rankkey.Zone(int32(zoneID), board), 0, 9).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "zone_id": zoneID, "list": vals})
}

func (s *Server) handleGlobalTop(w http.ResponseWriter, r *http.Request) {
	board := r.URL.Query().Get("board")
	if board == "" {
		board = "dungeon"
	}
	vals, err := s.redis.ZRevRangeWithScores(r.Context(), rankkey.Global(board), 0, 19).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "list": vals})
}

func (s *Server) Run() error {
	s.log.Info("rank service ready", zap.String("addr", s.cfg.HTTPAddr))
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.nats != nil {
		closers = append(closers, app.CloseNoContext(s.nats.Drain))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		go s.trimLoop(ctx)
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
