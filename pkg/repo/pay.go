package repo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OrderStatusPending = 0 // 待支付
	OrderStatusPaid    = 1 // 已支付（可发货）
)

// Order 支付订单记录。
type Order struct {
	Currency              string
	ID                    string // 订单号，如 ord_10001_1
	RoleID                int64  // 购买角色 ID
	ProductID             string // 商品 ID，如 coin_pack
	Amount                int32  // 支付金额（分或游戏内单位）
	Status                int32  // OrderStatusPending / OrderStatusPaid
	Delivered             bool   // 游戏内道具是否已发放
	ProviderTransactionID string // 支付平台唯一交易号
}

// PayRepo 订单创建、支付回调、发货标记与对账。
type PayRepo struct {
	pool *pgxpool.Pool
}

// NewPayRepo 创建支付仓库。
func NewPayRepo(pool *pgxpool.Pool) *PayRepo {
	return &PayRepo{pool: pool}
}

func (r *PayRepo) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// CreateOrder 插入新订单，delivered 默认为 false。
//
// 参数:
//   - ctx: 数据库上下文
//   - o: 订单信息；Status 通常为 OrderStatusPending
func (r *PayRepo) CreateOrder(ctx context.Context, o Order) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO orders (id, role_id, product_id, amount, currency, status, delivered) VALUES ($1, $2, $3, $4, $5, $6, false)`,
		o.ID, o.RoleID, o.ProductID, o.Amount, o.Currency, o.Status,
	)
	return err
}

// MarkPaid 幂等地将订单标记为已支付（仅当当前 status 不是已付时更新）。
//
// 参数:
//   - ctx: 数据库上下文
//   - orderID: 订单号
//   - status: 目标状态，通常为 OrderStatusPaid
//
// 返回:
//   - Order: 最新订单数据
//   - bool: true 表示本次调用 newly 标记为已付；false 表示之前已是已付
//   - error: 订单不存在或数据库错误
func (r *PayRepo) MarkPaid(ctx context.Context, orderID string, status int32) (Order, bool, error) {
	var o Order
	err := r.pool.QueryRow(ctx,
		`UPDATE orders SET status = $2
		 WHERE id = $1 AND status <> $3
		 RETURNING id, role_id, product_id, amount, currency, status, delivered`,
		orderID, status, OrderStatusPaid,
	).Scan(&o.ID, &o.RoleID, &o.ProductID, &o.Amount, &o.Currency, &o.Status, &o.Delivered)
	if err == pgx.ErrNoRows {
		o, err2 := r.Get(ctx, orderID)
		return o, false, err2
	}
	return o, true, err
}

// MarkPaidVerified atomically verifies immutable order attributes and binds a
// provider transaction id. The unique provider id prevents the same payment
// from being applied to two local orders across Pay replicas.
func (r *PayRepo) MarkPaidVerified(
	ctx context.Context,
	orderID, providerTransactionID, productID, currency string,
	amount, status int32,
) (Order, bool, error) {
	if status != OrderStatusPaid || providerTransactionID == "" {
		return Order{}, false, fmt.Errorf("invalid paid notification")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Order{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var o Order
	err = tx.QueryRow(ctx,
		`SELECT id, role_id, product_id, amount, currency, status, delivered,
		        COALESCE(provider_transaction_id, '')
		   FROM orders WHERE id = $1 FOR UPDATE`, orderID,
	).Scan(&o.ID, &o.RoleID, &o.ProductID, &o.Amount, &o.Currency, &o.Status, &o.Delivered, &o.ProviderTransactionID)
	if err != nil {
		return Order{}, false, err
	}
	if o.ProductID != productID || o.Amount != amount || o.Currency != currency {
		return Order{}, false, fmt.Errorf("payment attributes do not match order")
	}
	if o.ProviderTransactionID != "" && o.ProviderTransactionID != providerTransactionID {
		return Order{}, false, fmt.Errorf("order already bound to another provider transaction")
	}
	newlyPaid := o.Status != OrderStatusPaid
	_, err = tx.Exec(ctx,
		`UPDATE orders
		    SET status = $2, provider_transaction_id = $3, paid_at = COALESCE(paid_at, NOW())
		  WHERE id = $1`,
		orderID, OrderStatusPaid, providerTransactionID,
	)
	if err != nil {
		return Order{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, err
	}
	o.Status = OrderStatusPaid
	o.ProviderTransactionID = providerTransactionID
	return o, newlyPaid, nil
}

// MarkDelivered 将订单标记为已发货（delivered=true）。
//
// 参数:
//   - ctx: 数据库上下文
//   - orderID: 订单号
func (r *PayRepo) MarkDelivered(ctx context.Context, orderID string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE orders SET delivered = true, delivered_at = COALESCE(delivered_at, NOW())
		  WHERE id = $1 AND status = $2`, orderID, OrderStatusPaid)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("paid order not found")
	}
	return err
}

// Get 按订单号查询单条订单。
func (r *PayRepo) Get(ctx context.Context, orderID string) (Order, error) {
	var o Order
	err := r.pool.QueryRow(ctx,
		`SELECT id, role_id, product_id, amount, currency, status, delivered,
		        COALESCE(provider_transaction_id, '')
		   FROM orders WHERE id = $1`, orderID,
	).Scan(&o.ID, &o.RoleID, &o.ProductID, &o.Amount, &o.Currency, &o.Status, &o.Delivered, &o.ProviderTransactionID)
	return o, err
}

// ListUndeliveredPaid 列出已支付但未发货的订单，供 Pay retry 补发。
//
// 参数:
//   - ctx: 数据库上下文
//   - limit: 最大条数；<=0 时默认 50
func (r *PayRepo) ListUndeliveredPaid(ctx context.Context, limit int) ([]Order, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, role_id, product_id, amount, currency, status, delivered,
		        COALESCE(provider_transaction_id, '') FROM orders
		 WHERE status = $1 AND delivered = false ORDER BY created_at LIMIT $2`,
		OrderStatusPaid, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.RoleID, &o.ProductID, &o.Amount, &o.Currency, &o.Status, &o.Delivered, &o.ProviderTransactionID); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ReconcileSummary GM/运营支付对账汇总指标。
type ReconcileSummary struct {
	Total           int64 `json:"total"`            // 订单总数
	Pending         int64 `json:"pending"`          // 待支付数
	Paid            int64 `json:"paid"`             // 已支付数
	Delivered       int64 `json:"delivered"`        // 已发货数
	UndeliveredPaid int64 `json:"undelivered_paid"` // 已付未发货数
	AmountTotal     int64 `json:"amount_total"`     // 全部订单金额合计
	AmountPaid      int64 `json:"amount_paid"`      // 已支付订单金额合计
}

// Reconcile 统计 orders 表各状态数量与金额。
func (r *PayRepo) Reconcile(ctx context.Context) (ReconcileSummary, error) {
	var s ReconcileSummary
	err := r.pool.QueryRow(ctx, `
		SELECT
		 COUNT(*)::bigint,
		 COUNT(*) FILTER (WHERE status = 0)::bigint,
		 COUNT(*) FILTER (WHERE status = $1)::bigint,
		 COUNT(*) FILTER (WHERE delivered = true)::bigint,
		 COUNT(*) FILTER (WHERE status = $1 AND delivered = false)::bigint,
		 COALESCE(SUM(amount), 0)::bigint,
		 COALESCE(SUM(amount) FILTER (WHERE status = $1), 0)::bigint
		FROM orders`, OrderStatusPaid,
	).Scan(&s.Total, &s.Pending, &s.Paid, &s.Delivered, &s.UndeliveredPaid, &s.AmountTotal, &s.AmountPaid)
	return s, err
}
