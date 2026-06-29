// Package internal 邮件服务：收发、领取附件；已读与未领状态分离。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/protocol"
	"newgame/pkg/repo"
	redisx "newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Server struct {
	cfg        config.Service
	log        *zap.Logger
	redis      goredis.UniversalClient
	nats       *nats.Conn
	mail       *repo.MailRepo
	disc       *discovery.Registry
	game       *client.GameClient
	notify     *client.NotifyClient
	mem        map[int64][]repo.Mail
	nextMemID  int64
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	disc := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	s := &Server{
		cfg:   cfg,
		log:   logger,
		redis: rdb,
		disc:  disc,
		game:   client.NewGameClientSharded(disc, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret),
		notify: client.NewNotifyClient(rdb).WithSecret(cfg.InternalSecret),
		mem:    map[int64][]repo.Mail{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.mail = repo.NewMailRepo(pool)
		} else {
			logger.Warn("postgres connect failed", zap.Error(err))
		}
	}
	if cfg.Infra.NATS != "" {
		if nc, err := mq.Connect(cfg.Infra.NATS); err == nil {
			s.nats = nc
			_, _ = nc.Subscribe(mq.SubjectMailSend, func(m *nats.Msg) {
				var req pb.MailSendRequest
				if json.Unmarshal(m.Data, &req) == nil {
					_ = s.store(context.Background(), &req)
				}
			})
		}
	}
	return s, nil
}

func (s *Server) store(ctx context.Context, req *pb.MailSendRequest) error {
	m := repo.Mail{RoleID: req.RoleId, Title: req.Title, Content: req.Content, Items: req.Items}
	if s.mail != nil {
		if err := s.mail.Insert(ctx, m); err != nil {
			return err
		}
	} else {
		id := atomic.AddInt64(&s.nextMemID, 1)
		m.ID = id
		s.mem[req.RoleId] = append(s.mem[req.RoleId], m)
	}
	s.pushNewMail(ctx, req.RoleId, req.Title)
	return nil
}

// pushNewMail 若收件人在线，推送新邮件提醒（不在线时静默忽略）。
func (s *Server) pushNewMail(ctx context.Context, roleID int64, title string) {
	if s.notify == nil {
		return
	}
	body, _ := json.Marshal(map[string]any{"title": title})
	if err := s.notify.Push(ctx, roleID, protocol.CmdPush, protocol.ActPushMail, body); err != nil && err != client.ErrOffline {
		s.log.Warn("push new mail failed", zap.Int64("role", roleID), zap.Error(err))
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/mail/send", s.handleSend)
	mux.HandleFunc("/api/mail/list", s.handleList)
	mux.HandleFunc("/api/mail/claim", s.handleClaim)
	mux.HandleFunc("/api/mail/claim-all", s.handleClaimAll)
	mux.HandleFunc("/api/mail/read", s.handleRead)
	mux.HandleFunc("/api/mail/read-all", s.handleReadAll)
	mux.HandleFunc("/api/mail/unread", s.handleUnread)
	return mux
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req pb.MailSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	_ = s.store(r.Context(), &req)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	roleID, _ := strconv.ParseInt(r.URL.Query().Get("role_id"), 10, 64)
	if roleID == 0 {
		roleID = 10001
	}
	if s.mail != nil {
		list, err := s.mail.List(r.Context(), roleID, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "mails": list})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "mails": s.mem[roleID]})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId int64 `json:"role_id"`
		MailId int64 `json:"mail_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	m, err := s.getMail(r.Context(), req.MailId, req.RoleId)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "mail not found"})
		return
	}
	if m.Claimed {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1002, "message": "already claimed"})
		return
	}
	if m.Items != "" {
		if err := s.game.GrantItems(r.Context(), req.RoleId, m.Items, fmt.Sprintf("mail:%d", m.ID)); err != nil {
			s.log.Warn("mail grant failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 5000, "message": err.Error()})
			return
		}
	}
	if err := s.markClaimed(r.Context(), req.MailId, req.RoleId); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "items": m.Items})
}

func (s *Server) handleClaimAll(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId int64 `json:"role_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	claimed, failed, err := s.claimAll(r.Context(), req.RoleId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "claimed": claimed, "failed": failed})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId int64 `json:"role_id"`
		MailId int64 `json:"mail_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if err := s.markRead(r.Context(), req.MailId, req.RoleId); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleReadAll(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId int64 `json:"role_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	n, err := s.markReadAll(r.Context(), req.RoleId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "read": n})
}

func (s *Server) handleUnread(w http.ResponseWriter, r *http.Request) {
	roleID, _ := strconv.ParseInt(r.URL.Query().Get("role_id"), 10, 64)
	if roleID == 0 {
		roleID = 10001
	}
	unread, unclaimed, err := s.countUnread(r.Context(), roleID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "unread": unread, "unclaimed": unclaimed})
}

func (s *Server) claimAll(ctx context.Context, roleID int64) (claimed, failed int, err error) {
	if s.mail != nil {
		list, err := s.mail.ListUnclaimed(ctx, roleID, 50)
		if err != nil {
			return 0, 0, err
		}
		var ids []int64
		for _, m := range list {
			if m.Items == "" {
				ids = append(ids, m.ID)
				continue
			}
			if err := s.game.GrantItems(ctx, roleID, m.Items, fmt.Sprintf("mail:%d", m.ID)); err != nil {
				failed++
				continue
			}
			ids = append(ids, m.ID)
			claimed++
		}
		if len(ids) > 0 {
			_ = s.mail.MarkClaimedBatch(ctx, ids)
		}
		return claimed, failed, nil
	}
	for i := range s.mem[roleID] {
		m := &s.mem[roleID][i]
		if m.Claimed || m.Items == "" {
			continue
		}
		if err := s.game.GrantItems(ctx, roleID, m.Items, fmt.Sprintf("mail:%d", m.ID)); err != nil {
			failed++
			continue
		}
		m.Claimed = true
		claimed++
	}
	return claimed, failed, nil
}

func (s *Server) markRead(ctx context.Context, mailID, roleID int64) error {
	if s.mail != nil {
		return s.mail.MarkRead(ctx, mailID, roleID)
	}
	list := s.mem[roleID]
	for i := range list {
		if list[i].ID == mailID {
			list[i].Read = true
			s.mem[roleID] = list
			return nil
		}
	}
	return fmt.Errorf("not found")
}

func (s *Server) markReadAll(ctx context.Context, roleID int64) (int64, error) {
	if s.mail != nil {
		return s.mail.MarkReadAll(ctx, roleID)
	}
	var n int64
	for i := range s.mem[roleID] {
		if !s.mem[roleID][i].Read {
			s.mem[roleID][i].Read = true
			n++
		}
	}
	return n, nil
}

func (s *Server) countUnread(ctx context.Context, roleID int64) (unread, unclaimed int64, err error) {
	if s.mail != nil {
		return s.mail.CountUnreadUnclaimed(ctx, roleID)
	}
	for _, m := range s.mem[roleID] {
		if !m.Read {
			unread++
		}
		if !m.Claimed && m.Items != "" {
			unclaimed++
		}
	}
	return unread, unclaimed, nil
}

func (s *Server) getMail(ctx context.Context, mailID, roleID int64) (repo.Mail, error) {
	if s.mail != nil {
		return s.mail.Get(ctx, mailID, roleID)
	}
	for _, m := range s.mem[roleID] {
		if m.ID == mailID {
			return m, nil
		}
	}
	return repo.Mail{}, fmt.Errorf("not found")
}

func (s *Server) markClaimed(ctx context.Context, mailID, roleID int64) error {
	if s.mail != nil {
		return s.mail.MarkClaimed(ctx, mailID)
	}
	list := s.mem[roleID]
	for i := range list {
		if list[i].ID == mailID {
			list[i].Claimed = true
			s.mem[roleID] = list
			return nil
		}
	}
	return fmt.Errorf("not found")
}

func (s *Server) Run() error {
	ctx := context.Background()
	_ = redisx.Ping(ctx, s.redis)
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
