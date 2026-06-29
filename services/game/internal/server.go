// Package internal Game 服务 HTTP 入口：玩家消息、副本、发奖、公会、Boss、拍卖等。
package internal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/grant"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/protocol"
	"newgame/pkg/repo"
	redisx "newgame/pkg/redis"
	"newgame/services/game/internal/auction"
	"newgame/services/game/internal/dungeon"
	grantsvc "newgame/services/game/internal/grant"
	"newgame/services/game/internal/guild"
	"newgame/services/game/internal/guildwar"
	"newgame/services/game/internal/player"
	"newgame/services/game/internal/worldboss"

	goredis "github.com/redis/go-redis/v9"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Server struct {
	cfg        config.Service
	log        *zap.Logger
	redis      goredis.UniversalClient
	nats       *nats.Conn
	players    *player.Manager
	dungeon    *dungeon.Service
	grant      *grantsvc.Service
	guilds     *guild.Service
	guildwar   *guildwar.Service
	worldboss  *worldboss.Service
	auction    *auction.Service
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	var nc *nats.Conn
	if cfg.Infra.NATS != "" {
		c, err := mq.Connect(cfg.Infra.NATS)
		if err != nil {
			logger.Warn("nats connect failed", zap.Error(err))
		} else {
			nc = c
		}
	}
	var roles *repo.RoleRepo
	var guilds *repo.GuildRepo
	var auctions *repo.AuctionRepo
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if len(cfg.Infra.PostgresShards) > 0 {
		// 分库模式：角色数据按 role_id 分库；公会/拍卖等全局表仍用首个库。
		sp, err := db.NewShardedPool(ctx, cfg.Infra.PostgresShards)
		if err != nil {
			logger.Warn("postgres sharded connect failed", zap.Error(err))
		} else {
			roles = repo.NewRoleRepoSharded(sp)
			guilds = repo.NewGuildRepo(sp.All()[0])
			auctions = repo.NewAuctionRepo(sp.All()[0])
			logger.Info("postgres sharded enabled", zap.Int("shards", sp.Count()))
		}
	} else if cfg.Infra.Postgres != "" {
		pool, err := db.NewPool(ctx, cfg.Infra.Postgres)
		if err != nil {
			logger.Warn("postgres connect failed", zap.Error(err))
		} else {
			roles = repo.NewRoleRepo(pool)
			guilds = repo.NewGuildRepo(pool)
			auctions = repo.NewAuctionRepo(pool)
		}
	}
	players := player.NewManager(roles, player.PersistConfig{
		Mode:        cfg.Scale.SaveMode,
		Interval:    cfg.Scale.SaveInterval(),
		Concurrency: cfg.Scale.SaveConcurrency,
	})
	return &Server{
		cfg:       cfg,
		log:       logger,
		redis:     rdb,
		nats:      nc,
		players:   players,
		dungeon:   dungeon.New(cfg.ZoneID, nc),
		grant:     grantsvc.New(),
		guilds:    guild.New(guilds, cfg.ZoneID),
		guildwar:  guildwar.New(rdb),
		worldboss: worldboss.New(rdb),
		auction:   auction.New(auctions, players),
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
	s.players.Logout(ctx, req.Role)
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
	pl := s.players.Get(r.Context(), req.RoleID)
	result, err := s.dungeon.Pass(r.Context(), pl, req.DungeonID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = pl.Quests.AddProgress("main_1", 1)
	s.players.ScheduleSave(req.RoleID)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "level": result.Level, "gold": result.Gold})
}

func (s *Server) handleGrant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID int64            `json:"role_id"`
		Gold   int64              `json:"gold"`
		Items  map[string]int32   `json:"items"`
		Source string             `json:"source"`
		Raw    string             `json:"items_raw"`
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
	pl := s.players.Get(r.Context(), req.RoleID)
	if err := s.grant.Apply(r.Context(), pl, b, req.Source); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.players.SaveNow(r.Context(), req.RoleID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "gold": pl.Gold, "bag": pl.Inv})
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
	pl := s.players.Get(r.Context(), req.RoleID)
	pl.SetGuild(g.ID)
	s.players.ScheduleSave(req.RoleID)
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
		pl := s.players.Get(r.Context(), req.RoleID)
		pl.AddGold(10)
		s.players.ScheduleSave(req.RoleID)
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
		RoleID int64  `json:"role_id"`
		ItemID string `json:"item_id"`
		Qty    int32  `json:"qty"`
		Price  int64  `json:"price"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	l, err := s.auction.Create(r.Context(), req.RoleID, req.ItemID, req.Qty, req.Price)
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

func (s *Server) Run() error {
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		go s.runGRPC()
		err := app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
		// 收到关停信号、HTTP 退出后，把待落库玩家全部刷盘，避免丢数据。
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		s.players.FlushAll(ctx)
		s.log.Info("game flushed pending saves on shutdown")
		return err
	})
}
