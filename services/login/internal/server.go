// Package internal 登录服务：账号鉴权、会话签发、区服列表与 Gate 地址发现。
package internal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bastion/api/pb"
	"bastion/pkg/app"
	"bastion/pkg/config"
	"bastion/pkg/db"
	"bastion/pkg/discovery"
	"bastion/pkg/errors"
	"bastion/pkg/log"
	redisx "bastion/pkg/redis"
	"bastion/pkg/repo"
	"bastion/pkg/session"
	"bastion/pkg/zone"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const loginAttemptsPerMinute = 10

// loginRateScript 用单条 Lua 脚本原子完成计数和首次过期时间设置，
// 保证多个 Login 副本同时处理同一账号时仍共享一个固定窗口。
var loginRateScript = goredis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], 60)
end
return count
`)

type Server struct {
	cfg      config.Service
	log      *zap.Logger
	redis    goredis.UniversalClient
	pool     *pgxpool.Pool
	accounts *repo.AccountRepo
	roles    *repo.RoleRepo
	shards   *db.ShardedPool
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
		if cfg.Production() {
			return nil, fmt.Errorf("connect redis: %w", err)
		}
		logger.Warn("redis ping failed", zap.Error(err))
	}
	var pool *pgxpool.Pool
	accountDSN := cfg.Infra.Postgres
	if accountDSN == "" && len(cfg.Infra.PostgresShards) > 0 {
		accountDSN = cfg.Infra.PostgresShards[0]
	}
	if accountDSN != "" {
		p, err := db.NewPool(ctx, accountDSN)
		if err != nil {
			return nil, fmt.Errorf("connect postgres: %w", err)
		} else {
			pool = p
		}
	}
	var accounts *repo.AccountRepo
	if pool != nil {
		accounts = repo.NewAccountRepo(pool)
	}
	var roles *repo.RoleRepo
	var shards *db.ShardedPool
	if len(cfg.Infra.PostgresShards) > 0 {
		sp, err := db.NewShardedPool(ctx, cfg.Infra.PostgresShards)
		if err != nil {
			return nil, fmt.Errorf("connect role shards: %w", err)
		}
		shards = sp
		roles = repo.NewRoleRepoSharded(sp)
	} else if pool != nil {
		roles = repo.NewRoleRepo(pool)
	}
	reg := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	return &Server{
		cfg:      cfg,
		log:      logger,
		redis:    rdb,
		pool:     pool,
		accounts: accounts,
		roles:    roles,
		shards:   shards,
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
	if req.Username == "" || len(req.Username) > 64 || req.Password == "" || len(req.Password) > 72 {
		writeJSON(w, pb.LoginResponse{
			Code: int32(errors.CodeInvalidParam), Message: "invalid username or password length",
		})
		return
	}
	if limited, err := s.loginRateLimited(r.Context(), req.Username); err != nil {
		if s.cfg.Production() {
			http.Error(w, "login rate limiter unavailable", http.StatusServiceUnavailable)
			return
		}
		s.log.Warn("login rate limiter unavailable", zap.Error(err))
	} else if limited {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		writeJSON(w, pb.LoginResponse{
			Code: int32(errors.CodeRateLimited), Message: "too many login attempts",
		})
		return
	}
	ctx := r.Context()

	roleID, zoneID, err := s.resolveRole(ctx, &req)
	if err != nil {
		code := errors.CodeInternal
		if stderrors.Is(err, repo.ErrInvalidPassword) {
			code = errors.CodeUnauthorized
		}
		writeJSON(w, pb.LoginResponse{Code: int32(code), Message: err.Error()})
		return
	}

	gateAddr, err := s.gateAddr(ctx, zoneID)
	if err != nil {
		writeJSON(w, pb.LoginResponse{Code: int32(errors.CodeInternal), Message: err.Error()})
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
		GateAddr: gateAddr,
		Message:  "ok",
	})
}

// loginRateKey 先规范化并哈希用户名，避免把账号明文写进 Redis key。
// 花括号是 Redis Cluster hash-tag，使该账号的限流操作稳定落在同一 slot。
func loginRateKey(username string) string {
	normalized := strings.ToLower(strings.TrimSpace(username))
	sum := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("ng:login:rate:{%x}", sum[:16])
}

// loginRateLimited 按规范化用户名限制分布式密码尝试次数。
// 不直接采用来源 IP，是为了避免负载均衡或 NAT 下大量正常用户共用一个 IP。
func (s *Server) loginRateLimited(ctx context.Context, username string) (bool, error) {
	if s.redis == nil {
		return false, fmt.Errorf("redis unavailable")
	}
	count, err := loginRateScript.Run(ctx, s.redis, []string{loginRateKey(username)}).Int()
	return count > loginAttemptsPerMinute, err
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
		role, err := s.accounts.GetOrCreateRole(ctx, accountID, zoneID, req.Username, s.cfg.Scale.ShardCount)
		if err != nil {
			return 0, 0, err
		}
		if s.roles == nil {
			return 0, 0, fmt.Errorf("role storage unavailable")
		}
		if err := s.roles.Ensure(ctx, role); err != nil {
			return 0, 0, fmt.Errorf("ensure role shard: %w", err)
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
func (s *Server) gateAddr(ctx context.Context, zoneID int32) (string, error) {
	if zoneID == 0 {
		zoneID = s.cfg.ZoneID
	}
	if s.resolver != nil && s.cfg.Discovery.Enabled {
		if inst, ok := s.resolver.Resolve(ctx, "gate", zoneID); ok && inst.TCPAddr != "" {
			return inst.TCPAddr, nil
		}
		s.log.Warn("gate discovery failed", zap.Int32("zone", zoneID))
	}
	if s.cfg.Production() {
		return "", fmt.Errorf("gate unavailable for zone %d", zoneID)
	}
	switch zoneID {
	case 2:
		return "127.0.0.1:9010", nil
	default:
		return "127.0.0.1:9000", nil
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
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.shards != nil {
		closers = append(closers, app.CloseVoid(s.shards.Close))
	}
	if s.pool != nil {
		closers = append(closers, app.CloseVoid(s.pool.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
