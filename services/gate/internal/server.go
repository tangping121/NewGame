// Package internal 实现 Gate TCP 接入：鉴权、限流、协议转发至 Game。
package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/discovery"
	"newgame/pkg/errors"
	"newgame/pkg/gateforward"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/presence"
	"newgame/pkg/protocol"
	"newgame/pkg/session"
	"newgame/pkg/shard"
	redisx "newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type connState struct {
	roleID int64
	zoneID int32
	authed bool // 是否已完成 CmdLogin
}

// clientConn 包装一条客户端 TCP 连接，串行化写操作。
// dispatch 响应与服务端主动推送可能并发写同一连接，故用 mu 保护。
type clientConn struct {
	conn         net.Conn
	mu           sync.Mutex
	writeTimeout time.Duration
}

// write 线程安全地写出一帧，带写超时防止慢客户端拖住 goroutine。
func (c *clientConn) write(f protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	_, err := c.conn.Write(protocol.Encode(f))
	return err
}

type Server struct {
	cfg       config.Service
	log       *zap.Logger
	redis     goredis.UniversalClient
	disc      *discovery.Registry
	resolver  *discovery.Resolver   // Game 实例解析 TTL 缓存，避免每帧健康探测
	gameCli   *client.GameClient    // 仅用于下线通知 Game 卸载 Actor
	limiter   *connLimiter
	forward   gateforward.Forwarder // HTTP 连接池或 gRPC 池，由 game_transport 决定
	transport string                // "grpc" | "http_pool"
	gateInst  string                // 本 Gate 实例 ID，登录时写入 presence
	gateHTTP  string                // 本 Gate 对外 HTTP 地址，写入 presence 供跨分片推送
	conns     sync.Map              // roleID(int64) -> *clientConn，本 Gate 在线连接表
	closing   atomic.Bool           // 是否正在优雅停机
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	poolSize := cfg.Gate.GamePoolSize
	transport := cfg.Gate.GameTransport
	if transport == "" {
		transport = "http_pool"
	}
	var fwd gateforward.Forwarder
	if transport == "grpc" {
		gp := gateforward.NewGRPCPool()
		gp.SetSecret(cfg.InternalSecret)
		fwd = gp
	} else {
		hp := gateforward.NewHTTPPool(poolSize)
		hp.SetSecret(cfg.InternalSecret)
		fwd = hp
	}
	reg := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	srv := &Server{
		cfg:       cfg,
		log:       logger,
		redis:     rdb,
		disc:      reg,
		resolver:  discovery.NewResolver(reg, 2*time.Second),
		gameCli:   client.NewGameClientSharded(reg, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret),
		limiter:   newConnLimiter(cfg.MaxConnPerIP),
		forward:   fwd,
		transport: transport,
	}
	if cfg.Discovery.Enabled {
		srv.gateInst = discovery.InstanceFromConfig(cfg).ID
	}
	srv.gateHTTP = discovery.PublishAddr(cfg.HTTPAddr)
	return srv, nil
}

func (s *Server) Run() error {
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		ln, err := net.Listen("tcp", s.cfg.TCPAddr)
		if err != nil {
			return err
		}
		s.log.Info("gate tcp listening", zap.String("addr", s.cfg.TCPAddr))

		// 收到 SIGINT/SIGTERM 时关闭监听，停止 Accept，实现优雅停机。
		go func() {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
			<-ch
			s.closing.Store(true)
			_ = ln.Close()
		}()

		go func() {
			mux := http.NewServeMux()
			app.MountHealth(mux)
			mux.HandleFunc("/internal/push", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handlePush))
			_ = app.RunHTTP(s.log, s.cfg.HTTPAddr, mux)
		}()

		var wg sync.WaitGroup
		// 多 acceptor 协程并发 Accept，提升高建连速率下的接入吞吐。
		n := s.cfg.Gate.AcceptorCount()
		errCh := make(chan error, n)
		var accWG sync.WaitGroup
		for i := 0; i < n; i++ {
			accWG.Add(1)
			go func() {
				defer accWG.Done()
				errCh <- s.acceptLoop(ln, &wg)
			}()
		}
		accWG.Wait()
		wg.Wait() // 等待在处理的连接结束
		if s.closing.Load() {
			s.log.Info("gate stopped gracefully")
			return nil
		}
		return <-errCh
	})
}

// acceptLoop 单个 Accept 循环；监听关闭（优雅停机）时返回 nil。
func (s *Server) acceptLoop(ln net.Listener, wg *sync.WaitGroup) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			return err
		}
		if !s.limiter.acquire(conn.RemoteAddr()) {
			s.log.Warn("connection limit exceeded", zap.String("remote", conn.RemoteAddr().String()))
			_ = conn.Close()
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer s.limiter.release(c.RemoteAddr())
			s.handleConn(c)
		}(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	st := &connState{}
	cc := &clientConn{conn: conn, writeTimeout: s.cfg.Gate.WriteTimeout()}
	defer func() {
		if st.authed && st.roleID > 0 {
			s.conns.Delete(st.roleID)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = presence.Remove(ctx, s.redis, st.roleID)
			_ = s.gameCli.Logout(ctx, st.roleID) // 通知 Game 卸载 Actor，避免内存泄漏
			cancel()
		}
	}()
	reader := bufio.NewReader(conn)
	// 复用 header 与 body 缓冲，避免每帧两次分配带来的 GC 压力。
	hdr := make([]byte, 2)
	var body []byte
	readTimeout := s.cfg.Gate.ReadTimeout()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		if _, err := io.ReadFull(reader, hdr); err != nil {
			return
		}
		size := int(hdr[0])<<8 | int(hdr[1])
		if size < protocol.HeaderSize || size > 65535 {
			return
		}
		if cap(body) < size {
			body = make([]byte, size)
		} else {
			body = body[:size]
		}
		copy(body[0:2], hdr)
		if _, err := io.ReadFull(reader, body[2:]); err != nil {
			return
		}
		frame, err := protocol.Decode(body[2:])
		if err != nil {
			return
		}
		wasAuthed := st.authed
		resp := s.dispatch(context.Background(), st, frame)
		if !wasAuthed && st.authed && st.roleID > 0 {
			s.conns.Store(st.roleID, cc) // 登录成功后登记连接，供 /internal/push 使用
		}
		if err := cc.write(resp); err != nil {
			return
		}
	}
}

// handlePush POST /internal/push — 跨分片/服务端主动推送一帧给本 Gate 的在线玩家。
//
// 请求 JSON: { role_id, cmd, act, body }
// 响应 JSON: { code:0 } 成功；1003 玩家不在本 Gate
func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		RoleID int64  `json:"role_id"`
		Cmd    uint16 `json:"cmd"`
		Act    uint16 `json:"act"`
		Body   string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RoleID == 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Cmd == 0 {
		req.Cmd = protocol.CmdPush
	}
	v, ok := s.conns.Load(req.RoleID)
	if !ok {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": int(errors.CodeNotFound), "message": "player not on this gate"})
		return
	}
	cc := v.(*clientConn)
	if err := cc.write(protocol.Frame{Cmd: req.Cmd, Act: req.Act, Body: []byte(req.Body)}); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": int(errors.CodeInternal), "message": "write failed"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

// dispatch 按 Cmd 路由 TCP 帧；未登录时除 CmdLogin/CmdPing 外返回 1002。
func (s *Server) dispatch(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	switch f.Cmd {
	case protocol.CmdPing:
		if st.authed && st.roleID > 0 {
			_ = presence.Renew(ctx, s.redis, st.roleID, 24*time.Hour)
		}
		return protocol.Frame{Cmd: f.Cmd, Act: f.Act, Body: []byte(`{"pong":true}`)}
	case protocol.CmdLogin:
		return s.handleLogin(ctx, st, f)
	default:
		if !st.authed {
			return errFrame(f, errors.CodeUnauthorized, "not logged in")
		}
		return s.forwardGame(ctx, st, f)
	}
}

// handleLogin CmdLogin(1)+ActLogin(1)：校验 Redis session token，绑定 role_id/zone_id。
// 请求 JSON: protocol.LoginGateRequest { token }
// 成功 JSON: protocol.LoginGateResponse { code:0, message, zone_id }
// zone_mode=dedicated 时校验 session.zone_id 与本 Gate zone_id 一致。
func (s *Server) handleLogin(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	var req pb.EnterGateRequest
	if err := json.Unmarshal(f.Body, &req); err != nil || req.Token == "" {
		return errFrame(f, errors.CodeInvalidParam, "token required")
	}
	info, err := session.Load(ctx, s.redis, req.Token)
	if err != nil {
		return errFrame(f, errors.CodeUnauthorized, "invalid token")
	}
	if s.cfg.ZoneMode != "hub" && s.cfg.ZoneID > 0 && info.ZoneID > 0 && info.ZoneID != s.cfg.ZoneID {
		return errFrame(f, errors.CodeUnauthorized, "zone mismatch")
	}
	st.authed = true
	st.roleID = info.RoleID
	st.zoneID = info.ZoneID
	if st.zoneID == 0 {
		st.zoneID = s.cfg.ZoneID
	}
	shardCount := s.cfg.Scale.ShardCount
	if shardCount <= 0 {
		shardCount = 1
	}
	shardID := shard.ForRole(st.roleID, shardCount)
	_ = presence.Store(ctx, s.redis, presence.Record{
		RoleID:   st.roleID,
		ZoneID:   st.zoneID,
		ShardID:  shardID,
		GateID:   s.gateInst,
		GateAddr: discovery.PublishAddr(s.cfg.TCPAddr),
		GateHTTP: s.gateHTTP,
	}, 24*time.Hour)
	return protocol.Frame{
		Cmd:  f.Cmd,
		Act:  f.Act,
		Body: []byte(`{"code":0,"message":"gate login ok","zone_id":` + strconv.FormatInt(int64(st.zoneID), 10) + `}`),
	}
}

// forwardGame CmdGame(2)+Act*：按 game_transport 经 HTTP 连接池或 gRPC 转发至 Game 分片。
func (s *Server) forwardGame(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	target := s.gameTarget(ctx, st.roleID, st.zoneID)
	if target == "" {
		return errFrame(f, errors.CodeInternal, "game unavailable")
	}
	// 限定转发耗时，避免单个 Game 慢请求拖垮 Gate 连接 goroutine。
	fctx, cancel := context.WithTimeout(ctx, s.cfg.Gate.ForwardTimeout())
	defer cancel()
	b, err := s.forward.Forward(fctx, target, st.roleID, st.zoneID, f.Cmd, f.Act, f.Body)
	if err != nil {
		return errFrame(f, errors.CodeInternal, "game unavailable")
	}
	return protocol.Frame{Cmd: f.Cmd, Act: f.Act, Body: b}
}

// gameTarget 按 role_id 分片路由解析目标地址。
//
// transport=grpc 返回实例 gRPC host:port；否则返回 HTTP 基址。
// 未启用分片时回退到 type=game；发现失败时回退到本地端口。
func (s *Server) gameTarget(ctx context.Context, roleID int64, zoneID int32) string {
	if zoneID == 0 {
		zoneID = s.cfg.ZoneID
	}
	if s.resolver != nil && s.cfg.Discovery.Enabled {
		name := "game"
		if s.cfg.Scale.ShardCount > 0 {
			name = shard.ServiceName(shard.ForRole(roleID, s.cfg.Scale.ShardCount))
		}
		// 命中 TTL 缓存则零额外开销；未命中才查发现+健康探测。
		if inst, ok := s.resolver.Resolve(ctx, name, zoneID); ok {
			if s.transport == "grpc" {
				return inst.GRPCAddr
			}
			return inst.HTTPBase()
		}
		s.log.Warn("game discovery failed", zap.String("name", name), zap.Int32("zone", zoneID))
	}
	return s.fallbackTarget(zoneID)
}

// fallbackTarget 无服务发现时的本地回退地址。
func (s *Server) fallbackTarget(zoneID int32) string {
	if s.transport == "grpc" {
		switch zoneID {
		case 2:
			return "127.0.0.1:9160"
		default:
			return "127.0.0.1:9150"
		}
	}
	switch zoneID {
	case 2:
		return "http://127.0.0.1:9110"
	default:
		return "http://127.0.0.1:9100"
	}
}

func errFrame(f protocol.Frame, code errors.Code, msg string) protocol.Frame {
	b, _ := json.Marshal(map[string]any{"code": int(code), "message": msg})
	return protocol.Frame{Cmd: f.Cmd, Act: f.Act, Body: b}
}
