// Package internal 登录服务：账号鉴权、会话签发、区服列表与 Gate 地址发现。
package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/errors"
	"newgame/pkg/log"
	"newgame/pkg/repo"
	"newgame/pkg/session"
	redisx "newgame/pkg/redis"
	"newgame/pkg/zone"

	goredis "github.com/redis/go-redis/v9"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Server struct {
	cfg      config.Service
	log      *zap.Logger
	redis    goredis.UniversalClient
	pool     *pgxpool.Pool
	accounts *repo.AccountRepo
	resolver *discovery.Resolver // Gate 地址解析 TTL 缓存，避免登录高峰反复健康探测
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
		logger.Warn("redis ping failed", zap.Error(err))
	}
	var pool *pgxpool.Pool
	if cfg.Infra.Postgres != "" {
		p, err := db.NewPool(ctx, cfg.Infra.Postgres)
		if err != nil {
			logger.Warn("postgres connect failed", zap.Error(err))
		} else {
			pool = p
		}
	}
	var accounts *repo.AccountRepo
	if pool != nil {
		accounts = repo.NewAccountRepo(pool)
	}
	reg := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	return &Server{
		cfg:      cfg,
		log:      logger,
		redis:    rdb,
		pool:     pool,
		accounts: accounts,
		resolver: discovery.NewResolver(reg, 2*time.Second),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/login", s.handleLogin)
	mux.HandleFunc("/api/enter", s.handleEnter)
	mux.HandleFunc("/api/zones", s.handleZones)
	return mux
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	// POST /api/login
	// 请求 JSON: { username, password, zone_id }
	// 响应 JSON: { code, token, role_id, gate_addr, message }
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, pb.LoginResponse{Code: int32(errors.CodeInvalidParam), Message: err.Error()})
		return
	}
	if req.ZoneId <= 0 {
		req.ZoneId = s.cfg.ZoneID
	}
	ctx := r.Context()

	roleID, zoneID, err := s.resolveRole(ctx, &req)
	if err != nil {
		code := errors.CodeInternal
		if err.Error() == "invalid password" {
			code = errors.CodeUnauthorized
		}
		writeJSON(w, pb.LoginResponse{Code: int32(code), Message: err.Error()})
		return
	}

	token, err := newToken()
	if err != nil {
		writeJSON(w, pb.LoginResponse{Code: int32(errors.CodeInternal), Message: err.Error()})
		return
	}
	if err := session.Save(ctx, s.redis, token, session.Info{RoleID: roleID, ZoneID: zoneID}, 24*time.Hour); err != nil {
		writeJSON(w, pb.LoginResponse{Code: int32(errors.CodeInternal), Message: err.Error()})
		return
	}
	writeJSON(w, pb.LoginResponse{
		Code:     0,
		Token:    token,
		RoleId:   roleID,
		GateAddr: s.gateAddr(ctx, zoneID),
		Message:  "ok",
	})
}

// resolveRole 校验账号并返回该区服角色 ID。
//
// 参数:
//   - req.Username / req.Password: 登录凭据
//   - req.ZoneId: 目标区服；<=0 时使用服务默认 zone_id
//
// 返回: roleID, zoneID；密码错误或 DB 失败时 error
func (s *Server) resolveRole(ctx context.Context, req *pb.LoginRequest) (roleID int64, zoneID int32, err error) {
	zoneID = req.ZoneId
	if s.accounts != nil {
		accountID, err := s.accounts.Authenticate(ctx, req.Username, req.Password)
		if err != nil {
			return 0, 0, err
		}
		role, err := s.accounts.GetOrCreateRole(ctx, accountID, zoneID, req.Username)
		if err != nil {
			return 0, 0, err
		}
		return role.ID, zoneID, nil
	}
	// 无 Postgres 时的开发回退：固定角色 10001
	return 10001, zoneID, nil
}

func (s *Server) handleEnter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.EnterGateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]any{"code": errors.CodeInvalidParam, "message": err.Error()})
		return
	}
	info, err := session.Load(r.Context(), s.redis, req.Token)
	if err != nil {
		writeJSON(w, map[string]any{"code": errors.CodeUnauthorized, "message": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"code": 0, "role_id": info.RoleID, "zone_id": info.ZoneID})
}

func (s *Server) handleZones(w http.ResponseWriter, r *http.Request) {
	var list []map[string]any
	for _, z := range zone.Catalog {
		entry := map[string]any{"id": z.ID, "name": z.Name}
		if s.resolver != nil && s.cfg.Discovery.Enabled {
			// 区服列表接口会遍历所有区，使用 Resolver 缓存降低 Redis/健康检查压力。
			if inst, ok := s.resolver.Resolve(r.Context(), "gate", z.ID); ok {
				entry["gate_addr"] = inst.TCPAddr
				entry["online"] = true
			} else {
				entry["online"] = false
			}
		}
		list = append(list, entry)
	}
	writeJSON(w, map[string]any{"code": 0, "zones": list})
}

// gateAddr 通过服务发现获取目标区服的 Gate TCP 地址。
//
// 优先走 Resolver 缓存（默认 TTL 2s）；发现失败时回退到本地开发端口。
func (s *Server) gateAddr(ctx context.Context, zoneID int32) string {
	if zoneID == 0 {
		zoneID = s.cfg.ZoneID
	}
	if s.resolver != nil && s.cfg.Discovery.Enabled {
		if inst, ok := s.resolver.Resolve(ctx, "gate", zoneID); ok && inst.TCPAddr != "" {
			return inst.TCPAddr
		}
		s.log.Warn("gate discovery failed", zap.Int32("zone", zoneID))
	}
	switch zoneID {
	case 2:
		return "127.0.0.1:9010"
	default:
		return "127.0.0.1:9000"
	}
}

func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "tk_" + hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) Run() error {
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
