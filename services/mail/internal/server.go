// Package internal 邮件服务：收发、领取附件；已读与未领状态分离。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/mq"
	"newgame/pkg/protocol"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"
	"newgame/pkg/session"

	"github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Server struct {
	cfg       config.Service
	log       *zap.Logger
	redis     goredis.UniversalClient
	nats      *nats.Conn
	mail      *repo.MailRepo
	disc      *discovery.Registry
	game      *client.GameClient
	notify    *client.NotifyClient
	mem       map[int64][]repo.Mail
	memMu     sync.RWMutex
	nextMemID int64
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
		cfg:    cfg,
		log:    logger,
		redis:  rdb,
		disc:   disc,
		game:   client.NewGameClientSharded(disc, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret).WithStrict(cfg.Production()),
		notify: client.NewNotifyClient(rdb).WithSecret(cfg.InternalSecret),
		mem:    map[int64][]repo.Mail{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.mail = repo.NewMailRepo(pool)
		} else {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
	}
	if cfg.Infra.NATS != "" {
		if nc, err := mq.Connect(cfg.Infra.NATS); err == nil {
			s.nats = nc
			if _, err := mq.SubscribeDurable(nc, mq.SubjectMailSend, "mail-v1", func(ctx context.Context, event mq.Event) error {
				var req pb.MailSendRequest
				if err := json.Unmarshal(event.Data, &req); err != nil {
					return err
				}
				return s.storeEvent(ctx, event.EventID, &req)
			}); err != nil {
				nc.Close()
				return nil, fmt.Errorf("subscribe mail stream: %w", err)
			}
		} else if cfg.Production() {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
	}
	return s, nil
}

func (s *Server) storeEvent(ctx context.Context, eventID string, req *pb.MailSendRequest) error {
	if s.mail == nil {
		return s.store(ctx, req)
	}
	inserted, err := s.mail.InsertEvent(ctx, eventID, repo.Mail{
		RoleID: req.RoleId, Title: req.Title, Content: req.Content, Items: req.Items,
	})
	if err == nil && inserted {
		s.pushNewMail(ctx, req.RoleId, req.Title)
	}
	return err
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
		s.memMu.Lock()
		s.mem[req.RoleId] = append(s.mem[req.RoleId], m)
		s.memMu.Unlock()
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
	mux.HandleFunc("/api/mail/send", internalauth.HTTPMiddleware(s.cfg.InternalSecret, s.handleSend))
	auth := func(h http.HandlerFunc) http.HandlerFunc { return session.HTTPMiddleware(s.redis, h) }
	mux.HandleFunc("/api/mail/list", auth(s.handleList))
	mux.HandleFunc("/api/mail/claim", auth(s.handleClaim))
	mux.HandleFunc("/api/mail/claim-all", auth(s.handleClaimAll))
	mux.HandleFunc("/api/mail/read", auth(s.handleRead))
	mux.HandleFunc("/api/mail/read-all", auth(s.handleReadAll))
	mux.HandleFunc("/api/mail/unread", auth(s.handleUnread))
	return mux
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req pb.MailSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if err := s.store(r.Context(), &req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	roleID := session.RoleID(r.Context())
	if s.mail != nil {
		list, err := s.mail.List(r.Context(), roleID, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "mails": list})
		return
	}
	s.memMu.RLock()
	list := append([]repo.Mail(nil), s.mem[roleID]...)
	s.memMu.RUnlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "mails": list})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MailId int64 `json:"mail_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	roleID := session.RoleID(r.Context())
	m, err := s.getMail(r.Context(), req.MailId, roleID)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": "mail not found"})
		return
	}
	if m.Claimed {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1002, "message": "already claimed"})
		return
	}
	if m.Items != "" {
		if err := s.game.GrantItems(r.Context(), roleID, m.Items, fmt.Sprintf("mail:%d", m.ID)); err != nil {
			s.log.Warn("mail grant failed", zap.Error(err))
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 5000, "message": err.Error()})
			return
		}
	}
	if err := s.markClaimed(r.Context(), req.MailId, roleID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "items": m.Items})
}

func (s *Server) handleClaimAll(w http.ResponseWriter, r *http.Request) {
	claimed, failed, err := s.claimAll(r.Context(), session.RoleID(r.Context()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "claimed": claimed, "failed": failed})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MailId int64 `json:"mail_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if err := s.markRead(r.Context(), req.MailId, session.RoleID(r.Context())); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleReadAll(w http.ResponseWriter, r *http.Request) {
	n, err := s.markReadAll(r.Context(), session.RoleID(r.Context()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "read": n})
}

func (s *Server) handleUnread(w http.ResponseWriter, r *http.Request) {
	roleID := session.RoleID(r.Context())
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
			if err := s.mail.MarkClaimedBatch(ctx, ids); err != nil {
				return claimed, failed, err
			}
		}
		return claimed, failed, nil
	}
	s.memMu.Lock()
	defer s.memMu.Unlock()
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
	s.memMu.Lock()
	defer s.memMu.Unlock()
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
	s.memMu.Lock()
	defer s.memMu.Unlock()
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
	s.memMu.RLock()
	defer s.memMu.RUnlock()
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
	s.memMu.RLock()
	defer s.memMu.RUnlock()
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
	s.memMu.Lock()
	defer s.memMu.Unlock()
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
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.nats != nil {
		closers = append(closers, app.CloseNoContext(s.nats.Drain))
	}
	if s.mail != nil {
		closers = append(closers, app.CloseVoid(s.mail.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
