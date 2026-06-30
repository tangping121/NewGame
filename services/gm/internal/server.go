// Package internal 实现 GM 运维代理：转发到 Game/Mail/Pay 等内部接口。
package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/discovery"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/shard"
	"newgame/pkg/zone"
	redisx "newgame/pkg/redis"

	"go.uber.org/zap"
)

type Server struct {
	cfg      config.Service
	log      *zap.Logger
	disc     *discovery.Registry
	resolver *discovery.Resolver
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	var disc *discovery.Registry
	var resolver *discovery.Resolver
	if cfg.Infra.Redis != "" {
		disc = discovery.NewRegistry(redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster), cfg.Discovery.TTL())
		resolver = discovery.NewResolver(disc, 2*time.Second)
	}
	return &Server{cfg: cfg, log: log.New(cfg.LogLevel), disc: disc, resolver: resolver}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/gm/dungeon/pass", s.handleDungeonPass)
	mux.HandleFunc("/api/gm/mail/send", s.handleMailSend)
	mux.HandleFunc("/api/gm/grant", s.handleGrant)
	mux.HandleFunc("/api/gm/pay/reconcile", s.handlePayReconcile)
	mux.HandleFunc("/api/gm/season/guildwar/reset", s.handleGuildWarReset)
	mux.HandleFunc("/api/gm/season/worldboss/reset", s.handleWorldBossReset)
	mux.HandleFunc("/api/gm/services", s.handleServices)
	return mux
}

// zoneID 从查询参数解析目标区服，默认使用 GM 自身配置的 zone_id。
func (s *Server) zoneID(r *http.Request) int32 {
	if z, err := strconv.ParseInt(r.URL.Query().Get("zone_id"), 10, 32); err == nil && z > 0 {
		return int32(z)
	}
	return s.cfg.ZoneID
}

// proxy 将请求原样转发到指定服务的 HTTP 路径。
//
// 当 service=="game" 且启用分片时，解析 body 中的 role_id 路由到对应分片，
// 与 Gate / GameClient 保持一致，避免打到错误分片。
func (s *Server) proxy(w http.ResponseWriter, r *http.Request, method, service, path string) {
	body, _ := io.ReadAll(r.Body)
	base := s.serviceURL(r.Context(), service, s.zoneID(r))
	if service == "game" && s.cfg.Scale.ShardCount > 0 {
		if shardBase := s.gameShardURL(r.Context(), body, s.zoneID(r)); shardBase != "" {
			base = shardBase
		}
	}
	url := base + path
	req, err := http.NewRequestWithContext(r.Context(), method, url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if service == "game" {
		internalauth.SetHTTP(req, s.cfg.InternalSecret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleDungeonPass(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodPost, "game", "/internal/player/dungeon/pass")
}

func (s *Server) handleMailSend(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodPost, "mail", "/api/mail/send")
}

func (s *Server) handleGrant(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodPost, "game", "/internal/player/grant")
}

func (s *Server) handlePayReconcile(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodGet, "pay", "/api/pay/reconcile")
}

func (s *Server) handleGuildWarReset(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodPost, "game", "/internal/guildwar/reset")
}

func (s *Server) handleWorldBossReset(w http.ResponseWriter, r *http.Request) {
	s.proxy(w, r, http.MethodPost, "game", "/internal/worldboss/reset")
}

func (s *Server) handleServices(w http.ResponseWriter, r *http.Request) {
	type entry struct {
		Name     string `json:"name"`
		ZoneID   int32  `json:"zone_id"`
		HTTPAddr string `json:"http_addr"`
		TCPAddr  string `json:"tcp_addr"`
		ID       string `json:"id"`
	}
	var out []entry
	names := []string{"login", "gate", "game", "match", "battle", "social", "mail", "rank", "activity", "pay", "gm"}
	if s.disc != nil {
		for _, name := range names {
			list, err := s.disc.DiscoverAll(r.Context(), name)
			if err != nil {
				continue
			}
			for _, inst := range list {
				out = append(out, entry{
					Name: inst.Name, ZoneID: inst.ZoneID,
					HTTPAddr: inst.HTTPAddr, TCPAddr: inst.TCPAddr, ID: inst.ID,
				})
			}
		}
	}
	if out == nil {
		out = []entry{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "services": out, "zones": zone.Catalog})
}

// gameShardURL 解析请求体中的 role_id，返回其所在 Game 分片的 HTTP 基址。
// role_id 缺失或发现失败时返回 ""，由调用方回退到默认地址。
func (s *Server) gameShardURL(ctx context.Context, body []byte, zoneID int32) string {
	var probe struct {
		RoleID int64 `json:"role_id"`
	}
	if json.Unmarshal(body, &probe) != nil || probe.RoleID == 0 {
		return ""
	}
	if s.disc == nil {
		return ""
	}
	name := shard.ServiceName(shard.ForRole(probe.RoleID, s.cfg.Scale.ShardCount))
	if inst, ok := s.resolver.Resolve(ctx, name, zoneID); ok {
		return inst.HTTPBase()
	}
	return ""
}

func (s *Server) serviceURL(ctx context.Context, name string, zoneID int32) string {
	if s.resolver != nil && s.cfg.Discovery.Enabled {
		if inst, ok := s.resolver.Resolve(ctx, name, zoneID); ok {
			return inst.HTTPBase()
		}
		// 跨区服全局服务（如部分 Battle 部署）走 ResolveGlobal 缓存。
		if inst, ok := s.resolver.ResolveGlobal(ctx, name); ok {
			return inst.HTTPBase()
		}
		s.log.Warn("service discovery failed", zap.String("name", name), zap.Int32("zone", zoneID))
	}
	return fallbackURL(name, zoneID)
}

func fallbackURL(name string, zoneID int32) string {
	ports := map[string]map[int32]string{
		"game":  {1: "http://127.0.0.1:9100", 2: "http://127.0.0.1:9110"},
		"mail":  {1: "http://127.0.0.1:9500"},
		"pay":   {1: "http://127.0.0.1:9700"},
		"login": {1: "http://127.0.0.1:8080"},
	}
	if m, ok := ports[name]; ok {
		if u, ok := m[zoneID]; ok {
			return u
		}
		if u, ok := m[1]; ok {
			return u
		}
	}
	return ""
}

func (s *Server) Run() error {
	s.log.Info("gm service ready", zap.String("addr", s.cfg.HTTPAddr))
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
