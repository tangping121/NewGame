// Package internal 实现 Gate TCP 接入：鉴权、限流、协议转发至 Game。
package internal

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/discovery"
	"newgame/pkg/errors"
	"newgame/pkg/gateforward"
	"newgame/pkg/internalauth"
	"newgame/pkg/internaltls"
	"newgame/pkg/log"
	"newgame/pkg/presence"
	"newgame/pkg/protocol"
	redisx "newgame/pkg/redis"
	"newgame/pkg/session"
	"newgame/pkg/shard"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// presenceRenewInterval 限制单连接向 Redis 续约的频率。在线 TTL 为 24h，
// 踢线由登录时的跨 Gate kick 完成，心跳不必每次都打 Redis。
const presenceRenewInterval = 30 * time.Second

var pongBody = []byte(`{"pong":true}`)

var frameBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

type connState struct {
	roleID    int64
	zoneID    int32
	sessionID string
	authed    bool      // 是否已完成 CmdLogin
	nextRenew time.Time // 下次允许续约 presence 的时间
}

// clientConn 包装一条客户端 TCP 连接，串行化写操作。
// dispatch 响应与服务端主动推送可能并发写同一连接，故用 mu 保护。
type clientConn struct {
	conn         net.Conn
	mu           sync.Mutex
	writeTimeout time.Duration
	sessionID    string
}

// write 线程安全地写出一帧，带写超时防止慢客户端拖住 goroutine。
func (c *clientConn) write(f protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	bp := frameBufPool.Get().(*[]byte)
	buf, err := protocol.EncodeInto(*bp, f)
	if err != nil {
		frameBufPool.Put(bp)
		return err
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	_, err = c.conn.Write(buf)
	*bp = buf[:0]
	frameBufPool.Put(bp)
	return err
}

type Server struct {
	cfg       config.Service
	log       *zap.Logger
	redis     goredis.UniversalClient
	disc      *discovery.Registry
	resolver  *discovery.Resolver // Game 实例解析 TTL 缓存，避免每帧健康探测
	gameCli   *client.GameClient  // 仅用于下线通知 Game 卸载 Actor
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	poolSize := cfg.Gate.GamePoolSize
	transport := cfg.Gate.GameTransport
	if transport == "" {
		transport = "http_pool"
	}
	var fwd gateforward.Forwarder
	if transport == "grpc" {
		gp := gateforward.NewGRPCPool()
		gp.SetSecret(cfg.InternalSecret)
		if cfg.InternalTLS.Enabled {
			creds, err := internaltls.ClientCredentials(cfg.InternalTLS)
			if err != nil {
				return nil, err
			}
			gp.SetTransportCredentials(creds)
		}
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
		gameCli:   client.NewGameClientSharded(reg, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret).WithStrict(cfg.Production()),
		limiter:   newConnLimiter(cfg.MaxConnPerIP),
		forward:   fwd,
		transport: transport,
	}
	if cfg.Discovery.Enabled {
		srv.gateInst = discovery.InstanceFromConfig(cfg).ID
	}
	srv.gateHTTP = discovery.AdvertiseAddr(cfg.HTTPAddr, cfg.AdvertiseHTTPAddr)
	return srv, nil
}

func (s *Server) Run() error {
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	switch forward := s.forward.(type) {
	case interface{ Close() }:
		closers = append(closers, app.CloseVoid(forward.Close))
	case interface{ CloseIdleConnections() }:
		closers = append(closers, app.CloseVoid(forward.CloseIdleConnections))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		ln, err := net.Listen("tcp", s.cfg.TCPAddr)
		if err != nil {
			return err
		}
		s.log.Info("gate tcp listening", zap.String("addr", s.cfg.TCPAddr))
		group, groupCtx := errgroup.WithContext(ctx)
		group.Go(func() error {
			mux := http.NewServeMux()
			app.MountHealth(mux)
			mux.HandleFunc("/internal/push", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handlePush))
			mux.HandleFunc("/internal/kick", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleKick))
			return app.RunHTTPContext(groupCtx, s.log, s.cfg.HTTPAddr, mux)
		})
		var wg sync.WaitGroup
		group.Go(func() error {
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
			firstErr := <-errCh
			_ = ln.Close()
			accWG.Wait()
			wg.Wait()
			if s.closing.Load() {
				return nil
			}
			return firstErr
		})
		group.Go(func() error {
			<-groupCtx.Done()
			s.closing.Store(true)
			_ = ln.Close()
			s.closeConnections()
			return nil
		})
		err = group.Wait()
		s.log.Info("gate stopped gracefully")
		return err
	}, closers...)
}

func (s *Server) closeConnections() {
	s.conns.Range(func(_, value any) bool {
		_ = value.(*clientConn).conn.Close()
		return true
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
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetKeepAliveConfig(net.KeepAliveConfig{
				Enable:   true,
				Idle:     30 * time.Second,
				Interval: 15 * time.Second,
				Count:    4,
			})
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer s.limiter.release(c.RemoteAddr())
			app.GateConnections.Inc()
			defer app.GateConnections.Dec()
			s.handleConn(c)
		}(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// connCtx 绑定连接生命周期：读协程退出或写失败时 cancel，
	// 使进行中的 Game 转发（forwardGame）能提前结束，避免断线后仍占用 Game 算力。
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer conn.Close()

	st := &connState{}
	cc := &clientConn{conn: conn, writeTimeout: s.cfg.Gate.WriteTimeout()}
	defer func() {
		if st.authed && st.roleID > 0 && s.conns.CompareAndDelete(st.roleID, cc) {
			ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
			removed := true
			if err := presence.Remove(ctx, s.redis, st.roleID, st.sessionID); err != nil {
				removed = false
				redisx.RecordError("gate", "presence_remove")
				s.log.Warn("presence remove failed", zap.Int64("role", st.roleID), zap.Error(err))
			}
			if removed {
				if err := s.gameCli.Logout(ctx, st.roleID); err != nil {
					s.log.Warn("game logout failed", zap.Int64("role", st.roleID), zap.Error(err))
				}
			}
			c()
		}
	}()

	readTimeout := s.cfg.Gate.ReadTimeout()
	rate := s.cfg.Gate.MsgRate()
	frames := make(chan protocol.Frame, 8)

	// 独立读协程：客户端断线时关闭 channel 并 cancel connCtx，中断进行中的 Game 转发。
	go func() {
		defer close(frames)
		defer cancel()
		reader := bufio.NewReader(conn)
		hdr := make([]byte, 2)
		var body []byte
		var winStart time.Time
		var winCount int
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
			// body 会在下一帧复用。入队前拷贝载荷，避免后一帧覆盖通道里尚未处理的帧。
			if n := len(frame.Body); n > 0 {
				payload := make([]byte, n)
				copy(payload, frame.Body)
				frame.Body = payload
			}
			if rate > 0 {
				now := time.Now()
				if now.Sub(winStart) >= time.Second {
					winStart = now
					winCount = 0
				}
				winCount++
				if winCount > rate {
					select {
					case frames <- errFrame(frame, errors.CodeInvalidParam, "rate limited"):
					case <-connCtx.Done():
					}
					continue
				}
			}
			select {
			case frames <- frame:
			case <-connCtx.Done():
				return
			}
		}
	}()

	for frame := range frames {
		wasAuthed := st.authed
		resp := s.dispatch(connCtx, st, frame)
		if !wasAuthed && st.authed && st.roleID > 0 {
			cc.sessionID = st.sessionID
			if previous, loaded := s.conns.Swap(st.roleID, cc); loaded && previous != cc {
				_ = previous.(*clientConn).conn.Close()
			}
		}
		if err := cc.write(resp); err != nil {
			cancel()
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

// handleKick closes only the fenced connection named by session_id. A delayed
// kick from another Gate can therefore never disconnect a newer login.
func (s *Server) handleKick(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleID    int64  `json:"role_id"`
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RoleID <= 0 || req.SessionID == "" {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if current, ok := s.conns.Load(req.RoleID); ok {
		cc := current.(*clientConn)
		if cc.sessionID == req.SessionID {
			_ = cc.conn.Close()
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

// dispatch 按 Cmd 路由 TCP 帧；未登录时除 CmdLogin/CmdPing 外返回 1002。
func (s *Server) dispatch(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	switch f.Cmd {
	case protocol.CmdPing:
		if st.authed && st.roleID > 0 && !time.Now().Before(st.nextRenew) {
			if err := presence.Renew(ctx, s.redis, st.roleID, st.sessionID, 24*time.Hour); err != nil {
				redisx.RecordError("gate", "presence_renew")
				s.log.Debug("presence renew failed", zap.Int64("role", st.roleID), zap.Error(err))
				if err == presence.ErrStaleSession {
					st.authed = false
					return errFrame(f, errors.CodeUnauthorized, "session replaced")
				}
			} else {
				st.nextRenew = time.Now().Add(presenceRenewInterval)
			}
		}
		return protocol.Frame{Cmd: f.Cmd, Act: f.Act, Body: pongBody}
	case protocol.CmdLogin:
		return s.handleLogin(ctx, st, f)
	case protocol.CmdGame:
		if !st.authed {
			return errFrame(f, errors.CodeUnauthorized, "not logged in")
		}
		return s.forwardGame(ctx, st, f)
	default:
		return errFrame(f, errors.CodeInvalidParam, "unsupported command")
	}
}

// handleLogin CmdLogin(1)+ActLogin(1)：校验 Redis session token，绑定 role_id/zone_id。
// 请求 JSON: protocol.LoginGateRequest { token }
// 成功 JSON: protocol.LoginGateResponse { code:0, message, zone_id }
// zone_mode=dedicated 时校验 session.zone_id 与本 Gate zone_id 一致。
func (s *Server) handleLogin(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	if st.authed {
		return errFrame(f, errors.CodeInvalidParam, "already logged in")
	}
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
	sessionID, err := newConnectionSessionID()
	if err != nil {
		return errFrame(f, errors.CodeInternal, "failed to create session")
	}
	zoneID := info.ZoneID
	if zoneID == 0 {
		zoneID = s.cfg.ZoneID
	}
	shardCount := s.cfg.Scale.ShardCount
	if shardCount <= 0 {
		shardCount = 1
	}
	shardID := shard.ForRole(info.RoleID, shardCount)
	previous, err := presence.Store(ctx, s.redis, presence.Record{
		RoleID:    info.RoleID,
		ZoneID:    zoneID,
		ShardID:   shardID,
		GateID:    s.gateInst,
		GateAddr:  discovery.AdvertiseAddr(s.cfg.TCPAddr, s.cfg.AdvertiseTCPAddr),
		GateHTTP:  s.gateHTTP,
		SessionID: sessionID,
	}, 24*time.Hour)
	if err != nil {
		redisx.RecordError("gate", "presence_store")
		s.log.Warn("presence store failed", zap.Int64("role", info.RoleID), zap.Error(err))
		return errFrame(f, errors.CodeInternal, "presence unavailable")
	}
	st.authed = true
	st.roleID = info.RoleID
	st.zoneID = zoneID
	st.sessionID = sessionID
	st.nextRenew = time.Now().Add(presenceRenewInterval)
	if previous.SessionID != "" && previous.SessionID != sessionID && previous.GateHTTP != "" && previous.GateID != s.gateInst {
		go s.kickPrevious(previous)
	}
	return protocol.Frame{
		Cmd:  f.Cmd,
		Act:  f.Act,
		Body: []byte(`{"code":0,"message":"gate login ok","zone_id":` + strconv.FormatInt(int64(st.zoneID), 10) + `}`),
	}
}

func newConnectionSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (s *Server) kickPrevious(previous presence.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body, _ := json.Marshal(map[string]any{
		"role_id": previous.RoleID, "session_id": previous.SessionID,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+previous.GateHTTP+"/internal/kick", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	internalauth.SetHTTP(req, s.cfg.InternalSecret)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

// forwardGame CmdGame(2)+Act*：按 game_transport 经 HTTP 连接池或 gRPC 转发至 Game 分片。
//
// ctx 来自连接级 connCtx：客户端断线时读协程 cancel，进行中的转发会随 ctx 提前结束
// （另受 gate.forward_timeout 硬上限约束）。
func (s *Server) forwardGame(ctx context.Context, st *connState, f protocol.Frame) protocol.Frame {
	target := s.gameTarget(ctx, st.roleID, st.zoneID)
	if target == "" {
		return errFrame(f, errors.CodeInternal, "game unavailable")
	}
	// 限定转发耗时，避免单个 Game 慢请求拖垮 Gate 连接 goroutine。
	fctx, cancel := context.WithTimeout(ctx, s.cfg.Gate.ForwardTimeout())
	defer cancel()
	t0 := time.Now()
	b, err := s.forward.Forward(fctx, target, st.roleID, st.zoneID, f.Cmd, f.Act, f.Body)
	app.GateForwardLatency.Observe(time.Since(t0).Seconds())
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
	if s.cfg.Production() {
		return ""
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
