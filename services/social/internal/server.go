// Package internal 社交服务：跨区好友、世界频道聊天。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/log"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"
	"newgame/pkg/session"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const globalChannel = "global:world"

// maxMemChatMessages 无 Postgres 时内存聊天总条数上限，超出则丢弃最旧消息。
const maxMemChatMessages = 1000

type Server struct {
	cfg     config.Service
	log     *zap.Logger
	redis   goredis.UniversalClient
	social  *repo.SocialRepo
	memMu   sync.Mutex       // 保护 memChat（无 DB 时的开发回退）
	memChat []map[string]any // 仅 social==nil 时使用；生产应接 Postgres
}

func New(cfgPath string) (*Server, error) {
	var cfg config.Service
	if err := config.Load(cfgPath, &cfg); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogLevel)
	s := &Server{cfg: cfg, log: logger, redis: redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, s.redis, cfg.Production()); err != nil {
		_ = s.redis.Close()
		return nil, err
	}
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.social = repo.NewSocialRepo(pool)
		} else {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	auth := func(h http.HandlerFunc) http.HandlerFunc { return session.HTTPMiddleware(s.redis, h) }
	mux.HandleFunc("/api/social/friend/add", auth(s.handleFriendAdd))
	mux.HandleFunc("/api/social/friend/list", auth(s.handleFriendList))
	mux.HandleFunc("/api/social/chat/send", auth(s.handleChatSend))
	mux.HandleFunc("/api/social/chat/list", auth(s.handleChatList))
	mux.HandleFunc("/api/social/chat/global/send", auth(s.handleGlobalChatSend))
	mux.HandleFunc("/api/social/chat/global/list", auth(s.handleGlobalChatList))
	mux.HandleFunc("/api/social/guild/info", auth(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "guilds": map[int64]string{1: "default_guild"}})
	}))
	return mux
}

func (s *Server) handleFriendAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FriendId int64 `json:"friend_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.FriendId <= 0 {
		http.Error(w, "friend_id required", http.StatusBadRequest)
		return
	}
	info, _ := session.FromContext(r.Context())
	if s.social != nil {
		if err := s.social.AddFriend(r.Context(), info.RoleID, req.FriendId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.log.Info("cross-zone friend", zap.Int64("role", info.RoleID), zap.Int64("friend", req.FriendId), zap.Int32("zone", info.ZoneID))
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) handleFriendList(w http.ResponseWriter, r *http.Request) {
	roleID := session.RoleID(r.Context())
	if s.social != nil {
		list, err := s.social.ListFriends(r.Context(), roleID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "friends": list})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "friends": []int64{}})
}

func (s *Server) handleChatSend(w http.ResponseWriter, r *http.Request) {
	s.chatSend(w, r, r.URL.Query().Get("channel"))
}

func (s *Server) handleGlobalChatSend(w http.ResponseWriter, r *http.Request) {
	s.chatSend(w, r, globalChannel)
}

func (s *Server) chatSend(w http.ResponseWriter, r *http.Request, channel string) {
	var req struct {
		Channel string `json:"channel"`
		Text    string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	info, _ := session.FromContext(r.Context())
	if channel == "" {
		channel = req.Channel
	}
	if channel == "" {
		channel = "world"
	}
	text := req.Text
	if info.ZoneID > 0 {
		text = fmt.Sprintf("[z%d] %s", info.ZoneID, req.Text)
	}
	if s.social != nil {
		if err := s.social.InsertChat(r.Context(), channel, info.RoleID, text); err != nil {
			s.log.Warn("chat insert failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		s.appendMemChat(channel, info.RoleID, text)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "channel": channel})
}

func (s *Server) handleChatList(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	if channel == "" {
		channel = "world"
	}
	s.chatList(w, r, channel)
}

func (s *Server) handleGlobalChatList(w http.ResponseWriter, r *http.Request) {
	s.chatList(w, r, globalChannel)
}

func (s *Server) chatList(w http.ResponseWriter, r *http.Request, channel string) {
	if s.social != nil {
		list, err := s.social.RecentChat(r.Context(), channel, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "channel": channel, "messages": list})
		return
	}
	s.memMu.Lock()
	var filtered []map[string]any
	for _, m := range s.memChat {
		if m["channel"] == channel || (channel == globalChannel && m["channel"] == nil) {
			filtered = append(filtered, m)
		}
	}
	s.memMu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "channel": channel, "messages": filtered})
}

// appendMemChat 无 DB 时写入内存聊天，超过 maxMemChatMessages 时丢弃最旧记录。
func (s *Server) appendMemChat(channel string, roleID int64, text string) {
	s.memMu.Lock()
	defer s.memMu.Unlock()
	s.memChat = append(s.memChat, map[string]any{
		"channel": channel, "role_id": roleID, "text": text,
	})
	if len(s.memChat) > maxMemChatMessages {
		s.memChat = s.memChat[len(s.memChat)-maxMemChatMessages:]
	}
}

func (s *Server) Run() error {
	s.log.Info("social service ready", zap.String("addr", s.cfg.HTTPAddr))
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.social != nil {
		closers = append(closers, app.CloseVoid(s.social.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
