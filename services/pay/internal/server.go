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
	"strings"
	"sync"
	"time"

	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/internalauth"
	"newgame/pkg/log"
	"newgame/pkg/payment"
	redisx "newgame/pkg/redis"
	"newgame/pkg/repo"
	"newgame/pkg/session"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const orderStatusPaid = 1

// productRewards 商品 ID 到奖励字符串的映射。
var productRewards = map[string]string{
	"coin_pack": "gold:1000",
	"starter":   "gold:500,potion:5",
	"diamond":   "gold:200,gem:10",
}

var defaultProductPrices = map[string]int32{
	"coin_pack": 100,
	"starter":   600,
	"diamond":   300,
}

type Server struct {
	cfg           config.Service
	log           *zap.Logger
	redis         goredis.UniversalClient
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
		cfg:   cfg,
		log:   logger,
		redis: rdb,
		game:  client.NewGameClientSharded(disc, cfg.ZoneID, cfg.Scale.ShardCount).WithSecret(cfg.InternalSecret).WithStrict(cfg.Production()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisx.Require(ctx, rdb, cfg.Production()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
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
	mux.HandleFunc("/api/pay/notify", payment.VerifyWebhook(
		s.cfg.Payment.WebhookSecret, s.cfg.Payment.MaxSkew(), s.redis, s.handleNotify,
	))
	mux.HandleFunc("/api/pay/order/create", session.HTTPMiddleware(s.redis, s.handleCreate))
	mux.HandleFunc("/api/pay/retry", auth(s.handleRetry))
	mux.HandleFunc("/api/pay/reconcile", auth(s.handleReconcile))
	return mux
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProductId string `json:"product_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	roleID := session.RoleID(r.Context())
	amount := s.productPrice(req.ProductId)
	if roleID <= 0 || amount <= 0 || productRewards[req.ProductId] == "" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "invalid order"})
		return
	}
	if s.pay == nil {
		http.Error(w, "payment storage unavailable", http.StatusServiceUnavailable)
		return
	}
	orderID, err := newOrderID(roleID)
	if err != nil {
		http.Error(w, "failed to create order id", http.StatusInternalServerError)
		return
	}
	if err := s.pay.CreateOrder(r.Context(), repo.Order{
		ID: orderID, RoleID: roleID, ProductID: req.ProductId, Amount: amount,
		Currency: s.cfg.Payment.CurrencyCode(), Status: repo.OrderStatusPending,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": 0, "order_id": orderID, "amount": amount, "currency": s.cfg.Payment.CurrencyCode(),
	})
}

func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderID               string `json:"order_id"`
		RoleID                int64  `json:"role_id"`
		ProductID             string `json:"product_id"`
		Amount                int32  `json:"amount"`
		Currency              string `json:"currency"`
		Status                int32  `json:"status"`
		ProviderTransactionID string `json:"provider_transaction_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001})
		return
	}
	if req.Status != orderStatusPaid {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
		return
	}
	if req.OrderID == "" || req.ProviderTransactionID == "" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "order_id required"})
		return
	}
	if s.pay == nil {
		http.Error(w, "payment storage unavailable", http.StatusServiceUnavailable)
		return
	}
	lock := s.deliveryLock(req.OrderID)
	lock.Lock()
	defer lock.Unlock()
	order, _, err := s.pay.MarkPaidVerified(
		r.Context(), req.OrderID, req.ProviderTransactionID, req.ProductID,
		strings.ToUpper(strings.TrimSpace(req.Currency)), req.Amount, req.Status,
	)
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
	if req.RoleID != 0 && req.RoleID != order.RoleID {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 1001, "message": "role mismatch"})
		return
	}
	if err := s.deliver(r.Context(), order, req.OrderID); err != nil {
		s.log.Error("pay deliver failed", zap.Error(err), zap.String("order", req.OrderID))
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 5000, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0})
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

func (s *Server) productPrice(productID string) int32 {
	if price := s.cfg.Payment.Products[productID]; price > 0 {
		return price
	}
	return defaultProductPrices[productID]
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
	closers := []app.CloseFunc{app.CloseNoContext(s.redis.Close)}
	if s.pay != nil {
		closers = append(closers, app.CloseVoid(s.pay.Close))
	}
	return app.RunWithDiscoveryContext(s.cfg, s.log, func(ctx context.Context) error {
		return app.RunHTTPContext(ctx, s.log, s.cfg.HTTPAddr, s.Handler())
	}, closers...)
}
