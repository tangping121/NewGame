// Package internal Game 服务 HTTP 入口：玩家消息、副本、发奖、公会、Boss、拍卖等。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/grant"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/protocol"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"
	"newgame/services/game/internal/auction"
	"newgame/services/game/internal/dungeon"
	"newgame/services/game/internal/guild"
	"newgame/services/game/internal/guildwar"
	"newgame/services/game/internal/player"
	"newgame/services/game/internal/worldboss"

	"github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Server struct {
	cfg       config.Service
	log       *zap.Logger
	redis     goredis.UniversalClient
	nats      *nats.Conn
	players   *player.Manager
	dungeon   *dungeon.Service
	guilds    *guild.Service
	guildwar  *guildwar.Service
	worldboss *worldboss.Service
	auction   *auction.Service
	grpcSrv   *grpc.Server
	roles     *repo.RoleRepo
	closeDB   func()
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
	if err := redisx.Ping(ctx, rdb); err != nil {
		if cfg.Production() {
			return nil, fmt.Errorf("connect redis: %w", err)
		}
		logger.Warn("redis ping failed", zap.Error(err))
	}
	var nc *nats.Conn
	if cfg.Infra.NATS != "" {
		c, err := mq.Connect(cfg.Infra.NATS)
		if err != nil {
			if cfg.Production() {
				return nil, fmt.Errorf("connect nats: %w", err)
			}
			logger.Warn("nats connect failed", zap.Error(err))
		} else {
			nc = c
		}
	}
	var roles *repo.RoleRepo
	var guilds *repo.GuildRepo
	var auctions *repo.AuctionRepo
	var closeDB func()
	if len(cfg.Infra.PostgresShards) > 0 {
		// 分库模式：角色数据按 role_id 分库；公会/拍卖等全局表仍用首个库。
		sp, err := db.NewShardedPool(ctx, cfg.Infra.PostgresShards)
		if err != nil {
			return nil, fmt.Errorf("connect postgres shards: %w", err)
		} else {
			roles = repo.NewRoleRepoSharded(sp)
			guilds = repo.NewGuildRepo(sp.All()[0])
			auctions = repo.NewAuctionRepo(sp.All()[0])
			logger.Info("postgres sharded enabled", zap.Int("shards", sp.Count()))
			closeDB = sp.Close
		}
	} else if cfg.Infra.Postgres != "" {
		pool, err := db.NewPool(ctx, cfg.Infra.Postgres)
		if err != nil {
			return nil, fmt.Errorf("connect postgres: %w", err)
		} else {
			roles = repo.NewRoleRepo(pool)
			guilds = repo.NewGuildRepo(pool)
			auctions = repo.NewAuctionRepo(pool)
			closeDB = pool.Close
		}
	}
	players := player.NewManager(roles, player.PersistConfig{
		Mode:        cfg.Scale.SaveMode,
		Interval:    cfg.Scale.SaveInterval(),
		Concurrency: cfg.Scale.SaveConcurrency,
		ShardID:     cfg.Scale.ShardID,
		ShardCount:  cfg.Scale.ShardCount,
	})
	registry := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	gameClient := client.NewGameClientSharded(registry, cfg.ZoneID, cfg.Scale.ShardCount).
		WithSecret(cfg.InternalSecret).WithStrict(cfg.Production())
	return &Server{
		cfg:       cfg,
		log:       logger,
		redis:     rdb,
		nats:      nc,
		players:   players,
		dungeon:   dungeon.New(cfg.ZoneID, nc),
		guilds:    guild.New(guilds, cfg.ZoneID),
		guildwar:  guildwar.New(rdb),
		worldboss: worldboss.New(rdb),
		auction:   auction.New(auctions, players, gameClient),
		roles:     roles,
		closeDB:   closeDB,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	// auth 包裹内部接口；InternalSecret 为空时放行（开发）。
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return internalauth.HTTPMiddleware(s.cfg.InternalSecret, h)
	}
	mux.HandleFunc("/internal/player/msg", auth(s.handlePlayerMsg))
	mux.HandleFunc("/internal/player/logout", auth(s.handlePlayerLogout))
	mux.HandleFunc("/internal/player/dungeon/pass", auth(s.handleDungeonPass))
	mux.HandleFunc("/internal/player/grant", auth(s.handleGrant))
	mux.HandleFunc("/internal/guild/join", auth(s.handleGuildJoin))
	mux.HandleFunc("/internal/guild/info", auth(s.handleGuildInfo))
	mux.HandleFunc("/internal/worldboss/state", auth(s.handleWorldBossState))
	mux.HandleFunc("/internal/worldboss/attack", auth(s.handleWorldBossAttack))
	mux.HandleFunc("/internal/guildwar/state", auth(s.handleGuildWarState))
	mux.HandleFunc("/internal/guildwar/attack", auth(s.handleGuildWarAttack))
	mux.HandleFunc("/internal/auction/list", auth(s.handleAuctionList))
	mux.HandleFunc("/internal/auction/create", auth(s.handleAuctionCreate))
	mux.HandleFunc("/internal/auction/buy", auth(s.handleAuctionBuy))
	mux.HandleFunc("/internal/guildwar/reset", auth(s.handleGuildWarReset))
	mux.HandleFunc("/internal/worldboss/reset", auth(s.handleWorldBossReset))
	return mux
}

// handlePlayerLogout POST /internal/player/logout — Gate 断开连接时通知卸载玩家 Actor。
// 请求 JSON: { role_id }
func (s *Server) handlePlayerLogout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Role int64 `json:"role_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Role == 0 {
		http.Error(w, "role_id required", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.players.Logout(ctx, req.Role); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

// handlePlayerMsg POST /internal/player/msg — 执行 CmdGame 各 Act，见 protocol/payloads.go。
//
// cmd/act/role 从请求头读取，原始 payload 为 HTTP body（与 gateforward 约定，免双重编码）。
func (s *Server) handlePlayerMsg(w http.ResponseWriter, r *http.Request) {
	role, _ := strconv.ParseInt(r.Header.Get(protocol.HeaderRole), 10, 64)
	if role == 0 {
		http.Error(w, "role_id required", http.StatusBadRequest)
		return
	}
	cmd64, _ := strconv.ParseUint(r.Header.Get(protocol.HeaderCmd), 10, 16)
	act64, _ := strconv.ParseUint(r.Header.Get(protocol.HeaderAct), 10, 16)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp, err := s.players.HandleMsg(ctx, role, uint16(cmd64), uint16(act64), body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp)
}

func (s *Server) handleDungeonPass(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID    int64 `json:"role_id"`
		DungeonID int32 `json:"dungeon_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.RoleID == 0 {
		http.Error(w, "role_id required", http.StatusBadRequest)
		return
	}
	var result dungeon.PassResult
	err := s.players.WithPlayerOutbox(r.Context(), req.RoleID, func(pl *player.Actor) (repo.OutboxEvent, error) {
		var err error
		result, err = s.dungeon.Pass(r.Context(), pl, req.DungeonID)
		if err != nil {
			return repo.OutboxEvent{}, err
		}
		_, _ = pl.Quests.AddProgress("main_1", 1)
		eventData, marshalErr := json.Marshal(pb.RankUpdateRequest{
			ZoneId: s.cfg.ZoneID, RoleId: req.RoleID, Score: int64(result.Level), Board: "dungeon",
		})
		if marshalErr != nil {
			return repo.OutboxEvent{}, marshalErr
		}
		return repo.OutboxEvent{
			EventID: fmt.Sprintf("rank:dungeon:%d:%d", req.RoleID, result.Level),
			Subject: mq.SubjectRankUpdate, AggregateID: strconv.FormatInt(req.RoleID, 10),
			Payload: eventData,
		}, nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "level": result.Level, "gold": result.Gold})
}

func (s *Server) handleGrant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID int64            `json:"role_id"`
		Gold   int64            `json:"gold"`
		Items  map[string]int32 `json:"items"`
		Source string           `json:"source"`
		Raw    string           `json:"items_raw"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.RoleID == 0 {
		http.Error(w, "role_id required", http.StatusBadRequest)
		return
	}
	b := grant.Bundle{Gold: req.Gold, Items: req.Items}
	if req.Raw != "" {
		parsed, err := grant.Parse(req.Raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b.Gold += parsed.Gold
		for k, v := range parsed.Items {
			b.Items[k] += v
		}
	}
	if b.Items == nil {
		b.Items = map[string]int32{}
	}
	if req.Source == "" {
		http.Error(w, "source is required for economy mutation", http.StatusBadRequest)
		return
	}
	pl, applied, err := s.players.ApplyGrant(r.Context(), req.RoleID, b, req.Source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	snap := pl.Snapshot()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "duplicate": !applied, "gold": snap.Gold, "bag": snap.Bag,
	})
}

func (s *Server) handleGuildJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID  int64 `json:"role_id"`
		GuildID int64 `json:"guild_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.GuildID == 0 {
		req.GuildID = 1
	}
	g, err := s.guilds.Join(r.Context(), req.RoleID, req.GuildID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	if err := s.players.WithPlayerSaved(r.Context(), req.RoleID, func(pl *player.Actor) error {
		pl.SetGuild(g.ID)
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "guild_id": g.ID, "name": g.Name})
}

func (s *Server) handleGuildInfo(w http.ResponseWriter, r *http.Request) {
	guildID := int64(1)
	g, err := s.guilds.Info(r.Context(), guildID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "guild_id": g.ID, "name": g.Name, "members": g.Members,
	})
}

func (s *Server) handleWorldBossState(w http.ResponseWriter, r *http.Request) {
	st, err := s.worldboss.State(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "boss": st})
}

func (s *Server) handleWorldBossAttack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID int64 `json:"role_id"`
		Damage int64 `json:"damage"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	st, err := s.worldboss.Attack(r.Context(), req.RoleID, s.cfg.ZoneID, req.Damage)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if req.RoleID > 0 {
		if err := s.players.WithPlayerSaved(r.Context(), req.RoleID, func(pl *player.Actor) error {
			pl.AddGold(10)
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "boss": st})
}

func (s *Server) handleGuildWarState(w http.ResponseWriter, r *http.Request) {
	st, err := s.guildwar.State(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "war": st})
}

func (s *Server) handleGuildWarAttack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		GuildID int64 `json:"guild_id"`
		Damage  int64 `json:"damage"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.GuildID == 0 {
		req.GuildID = 1
	}
	st, err := s.guildwar.Attack(r.Context(), req.GuildID, req.Damage)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "war": st})
}

func (s *Server) handleAuctionList(w http.ResponseWriter, r *http.Request) {
	list, err := s.auction.List(r.Context(), 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "listings": list})
}

func (s *Server) handleAuctionCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID    int64  `json:"role_id"`
		ItemID    string `json:"item_id"`
		Qty       int32  `json:"qty"`
		Price     int64  `json:"price"`
		RequestID string `json:"request_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	l, err := s.auction.CreateWithKey(r.Context(), req.RoleID, req.ItemID, req.Qty, req.Price, req.RequestID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "listing": l})
}

func (s *Server) handleAuctionBuy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID    int64 `json:"role_id"`
		ListingID int64 `json:"listing_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	l, err := s.auction.Buy(r.Context(), req.RoleID, req.ListingID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "listing": l})
}

func (s *Server) handleGuildWarReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	st, err := s.guildwar.ResetSeason(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "war": st})
}

func (s *Server) handleWorldBossReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.worldboss.Reset(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	st, _ := s.worldboss.State(r.Context())
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "boss": st})
}

// reportMetrics 定期把在线数、落库队列深度与累计落库次数写入 Prometheus 指标。
func (s *Server) reportMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var lastSaved, lastFailed int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			app.GameOnline.Set(float64(s.players.Online()))
			if saver := s.players.Saver(); saver != nil {
				app.GameSaveQueueDepth.Set(float64(saver.QueueDepth()))
				saved, failed := saver.Stats()
				if d := saved - lastSaved; d > 0 {
					app.GameSaveTotal.Add(float64(d))
					lastSaved = saved
				}
				if d := failed - lastFailed; d > 0 {
					app.GameSaveFailed.Add(float64(d))
					lastFailed = failed
				}
			}
		}
	}
}

func (s *Server) runOutbox(ctx context.Context) {
	if s.roles == nil || s.nats == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.publishOutboxBatch(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("publish outbox failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) publishOutboxBatch(ctx context.Context) error {
	events, err := s.roles.ReserveOutbox(ctx, 100)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := mq.Publish(ctx, s.nats, event.Subject, mq.Event{
			EventID: event.EventID, AggregateID: event.AggregateID,
			AggregateVersion: event.AggregateVersion, Data: event.Payload,
		}); err != nil {
			return err
		}
		if err := s.roles.MarkOutboxPublished(ctx, event.PoolIndex, event.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Run() error {
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.nats != nil {
		closers = append(closers, app.CloseNoContext(s.nats.Drain))
	}
	if s.closeDB != nil {
		closers = append(closers, app.CloseVoid(s.closeDB))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		group, groupCtx := errgroup.WithContext(ctx)
		group.Go(func() error { return app.RunHTTPContext(groupCtx, s.log, s.cfg.HTTPAddr, s.Handler()) })
		group.Go(func() error { return s.runGRPC(groupCtx) })
		group.Go(func() error {
			s.reportMetrics(groupCtx)
			return nil
		})
		group.Go(func() error {
			s.runOutbox(groupCtx)
			return nil
		})
		err := group.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		s.players.StopAndFlush(ctx)
		s.log.Info("game flushed pending saves on shutdown")
		return err
	}, closers...)
}
