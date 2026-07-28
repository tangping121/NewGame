// Package internal 支付服务：下单、回调发货、对账与补发重试。
package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"

	"go.uber.org/zap"
)

const orderStatusPaid = 1

// productRewards 商品 ID 到奖励字符串的映射。
var productRewards = map[string]string{
	"coin_pack": "gold:1000",
	"starter":   "gold:500,potion:5",
	"diamond":   "gold:200,gem:10",
}

type Server struct {
	cfg           config.Service
	log           *zap.Logger
	pay           *repo.PayRepo
	game          *client.GameClient
	deliveryLocks [64]sync.Mutex
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
		cfg:  cfg,
		log:  logger,
		game: client.NewGameClientSharded(disc, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if cfg.Infra.Postgres != "" {
		if pool, err := db.NewPool(ctx, cfg.Infra.Postgres); err == nil {
			s.pay = repo.NewPayRepo(pool)
		} else {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return internalauth.HTTPMiddleware(s.cfg.InternalSecret, h)
	}
	mux.HandleFunc("/api/pay/notify", auth(s.handleNotify))
	mux.HandleFunc("/api/pay/order/create", s.handleCreate)
	mux.HandleFunc("/api/pay/retry", auth(s.handleRetry))
	mux.HandleFunc("/api/pay/reconcile", auth(s.handleReconcile))
	return mux
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoleId    int64  `json:"role_id"`
		ProductId string `json:"product_id"`
		Amount    int32  `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if req.RoleId <= 0 || req.Amount <= 0 || productRewards[req.ProductId] == "" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "invalid order"})
		return
	}
	if s.pay == nil {
		http.Error(w, "payment storage unavailable", http.StatusServiceUnavailable)
		return
	}
	orderID, err := newOrderID(req.RoleId)
	if err != nil {
		http.Error(w, "failed to create order id", http.StatusInternalServerError)
		return
	}
	if err := s.pay.CreateOrder(r.Context(), repo.Order{
		ID: orderID, RoleID: req.RoleId, ProductID: req.ProductId, Amount: req.Amount, Status: repo.OrderStatusPending,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "order_id": orderID})
}

func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var req pb.PayNotifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if req.Status != orderStatusPaid {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
		return
	}
	if req.OrderId == "" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "order_id required"})
		return
	}
	lock := s.deliveryLock(req.OrderId)
	lock.Lock()
	defer lock.Unlock()
	order, _, err := s.loadOrder(r.Context(), &req)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1003, "message": err.Error()})
		return
	}
	if order.Delivered {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "already delivered"})
		return
	}
	if order.Status != repo.OrderStatusPaid {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
		return
	}
	if err := s.deliver(r.Context(), order, req.OrderId); err != nil {
		s.log.Error("pay deliver failed", zap.Error(err), zap.String("order", req.OrderId))
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 5000, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
}

func (s *Server) loadOrder(ctx context.Context, req *pb.PayNotifyRequest) (repo.Order, bool, error) {
	if s.pay == nil {
		return repo.Order{}, false, fmt.Errorf("payment storage unavailable")
	}
	if req.OrderId != "" {
		return s.pay.MarkPaid(ctx, req.OrderId, req.Status)
	}
	return repo.Order{}, false, fmt.Errorf("order_id required")
}

func (s *Server) deliver(ctx context.Context, order repo.Order, orderID string) error {
	reward := productRewards[order.ProductID]
	if reward == "" {
		reward = fmt.Sprintf("gold:%d", order.Amount*10)
	}
	if err := s.game.GrantItems(ctx, order.RoleID, reward, "pay:"+orderID); err != nil {
		return err
	}
	if s.pay != nil {
		if err := s.pay.MarkDelivered(ctx, orderID); err != nil {
			return fmt.Errorf("mark order delivered: %w", err)
		}
	}
	s.log.Info("pay delivered", zap.String("order", orderID), zap.Int64("role", order.RoleID))
	return nil
}

func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	if s.pay == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "retried": 0})
		return
	}
	list, err := s.pay.ListUndeliveredPaid(r.Context(), 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var ok, fail int
	for _, o := range list {
		lock := s.deliveryLock(o.ID)
		lock.Lock()
		current, err := s.pay.Get(r.Context(), o.ID)
		if err == nil && current.Status == repo.OrderStatusPaid && !current.Delivered {
			err = s.deliver(r.Context(), current, current.ID)
		}
		lock.Unlock()
		if err != nil {
			fail++
			s.log.Warn("pay retry failed", zap.String("order", o.ID), zap.Error(err))
		} else {
			ok++
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "success": ok, "failed": fail})
}

func newOrderID(roleID int64) (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("ord_%d_%s", roleID, hex.EncodeToString(b[:])), nil
}

func (s *Server) deliveryLock(orderID string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(orderID))
	return &s.deliveryLocks[h.Sum32()%uint32(len(s.deliveryLocks))]
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if s.pay == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "summary": repo.ReconcileSummary{}})
		return
	}
	sum, err := s.pay.Reconcile(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "summary": sum})
}

func (s *Server) Run() error {
	return app.RunWithDiscovery(s.cfg, s.log, func() error {
		return app.RunHTTP(s.log, s.cfg.HTTPAddr, s.Handler())
	})
}
