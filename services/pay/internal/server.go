// Package internal 支付服务：下单、回调发货、对账与补发重试。
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"newgame/api/pb"
	"newgame/pkg/app"
	"newgame/pkg/client"
	"newgame/pkg/config"
	"newgame/pkg/db"
	"newgame/pkg/discovery"
	"newgame/pkg/log"
	"newgame/pkg/repo"
	redisx "newgame/pkg/redis"

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
	cfg  config.Service
	log  *zap.Logger
	pay  *repo.PayRepo
	game *client.GameClient
	seq  int64
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
			logger.Warn("postgres connect failed", zap.Error(err))
		}
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	app.MountHealth(mux)
	mux.HandleFunc("/api/pay/notify", s.handleNotify)
	mux.HandleFunc("/api/pay/order/create", s.handleCreate)
	mux.HandleFunc("/api/pay/retry", s.handleRetry)
	mux.HandleFunc("/api/pay/reconcile", s.handleReconcile)
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
	s.seq++
	orderID := fmt.Sprintf("ord_%d_%d", req.RoleId, s.seq)
	if s.pay != nil {
		_ = s.pay.CreateOrder(r.Context(), repo.Order{
			ID: orderID, RoleID: req.RoleId, ProductID: req.ProductId, Amount: req.Amount, Status: repo.OrderStatusPending,
		})
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
		return repo.Order{
			ID: req.OrderId, RoleID: req.RoleId, ProductID: req.ProductId, Amount: req.Amount, Status: orderStatusPaid,
		}, true, nil
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
		_ = s.pay.MarkDelivered(ctx, orderID)
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
		if err := s.deliver(r.Context(), o, o.ID); err != nil {
			fail++
			s.log.Warn("pay retry failed", zap.String("order", o.ID), zap.Error(err))
		} else {
			ok++
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "success": ok, "failed": fail})
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
