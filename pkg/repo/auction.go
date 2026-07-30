package repo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	AuctionOpen     = 0 // 在售
	AuctionSold     = 1 // 已售出
	AuctionCancel   = 2 // 已取消（预留）
	AuctionPending  = 3 // item escrow is being created
	AuctionReserved = 4 // buyer saga owns the listing
)

// AuctionListing 拍卖表 auction_listings 一行。
type AuctionListing struct {
	ID           int64     // 挂牌 ID
	SellerRoleID int64     // 卖家
	ItemID       string    // 道具 ID
	Qty          int32     // 数量
	Price        int64     // 售价（金币）
	Status       int32     // AuctionOpen / Sold / Cancel
	BuyerRoleID  int64     // 买家；未售时为 0
	CreatedAt    time.Time // 上架时间
	RequestKey   string    // idempotent create request
}

// GetOrCreatePending creates the durable saga before the seller item is
// removed. Retrying the same request key returns the same listing.
func (r *AuctionRepo) GetOrCreatePending(ctx context.Context, requestKey string, l AuctionListing) (AuctionListing, error) {
	var out AuctionListing
	err := r.pool.QueryRow(ctx,
		`INSERT INTO auction_listings
		    (seller_role_id, item_id, qty, price, status, request_key)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (request_key) WHERE request_key IS NOT NULL
		 DO UPDATE SET request_key = EXCLUDED.request_key
		 RETURNING id, seller_role_id, item_id, qty, price, status,
		           COALESCE(buyer_role_id, 0), created_at, request_key`,
		l.SellerRoleID, l.ItemID, l.Qty, l.Price, AuctionPending, requestKey,
	).Scan(&out.ID, &out.SellerRoleID, &out.ItemID, &out.Qty, &out.Price,
		&out.Status, &out.BuyerRoleID, &out.CreatedAt, &out.RequestKey)
	return out, err
}

func (r *AuctionRepo) Activate(ctx context.Context, id, sellerRoleID int64) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE auction_listings SET status = $3, updated_at = NOW()
		  WHERE id = $1 AND seller_role_id = $2 AND status IN ($4, $3)`,
		id, sellerRoleID, AuctionOpen, AuctionPending,
	)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// Reserve grants one buyer exclusive ownership of the settlement saga.
func (r *AuctionRepo) Reserve(ctx context.Context, id, buyerRoleID int64) (AuctionListing, error) {
	var l AuctionListing
	err := r.pool.QueryRow(ctx,
		`UPDATE auction_listings
		    SET status = $3, buyer_role_id = $2, updated_at = NOW()
		  WHERE id = $1
		    AND (status = $4 OR (status = $3 AND buyer_role_id = $2))
		  RETURNING id, seller_role_id, item_id, qty, price, status,
		            buyer_role_id, created_at, COALESCE(request_key, '')`,
		id, buyerRoleID, AuctionReserved, AuctionOpen,
	).Scan(&l.ID, &l.SellerRoleID, &l.ItemID, &l.Qty, &l.Price,
		&l.Status, &l.BuyerRoleID, &l.CreatedAt, &l.RequestKey)
	return l, err
}

// ReleaseReservation reopens a listing only when the named buyer still owns
// the reservation. It is used for deterministic failures such as low balance.
func (r *AuctionRepo) ReleaseReservation(ctx context.Context, id, buyerRoleID int64) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE auction_listings
		    SET status = $3, buyer_role_id = NULL, updated_at = NOW()
		  WHERE id = $1 AND buyer_role_id = $2 AND status = $4`,
		id, buyerRoleID, AuctionOpen, AuctionReserved,
	)
	_ = tag
	return err
}

// AuctionRepo 拍卖挂牌 CRUD。
type AuctionRepo struct {
	pool *pgxpool.Pool
}

func NewAuctionRepo(pool *pgxpool.Pool) *AuctionRepo {
	return &AuctionRepo{pool: pool}
}

// Create 插入新挂牌，status 固定为 AuctionOpen。
//
// 返回: 新记录自增 id
func (r *AuctionRepo) Create(ctx context.Context, l AuctionListing) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`INSERT INTO auction_listings (seller_role_id, item_id, qty, price, status)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		l.SellerRoleID, l.ItemID, l.Qty, l.Price, AuctionOpen,
	).Scan(&id)
	return id, err
}

// Get 按挂牌 ID 查询。
func (r *AuctionRepo) Get(ctx context.Context, id int64) (AuctionListing, error) {
	var l AuctionListing
	err := r.pool.QueryRow(ctx,
		`SELECT id, seller_role_id, item_id, qty, price, status, COALESCE(buyer_role_id, 0), created_at,
		        COALESCE(request_key, '')
		 FROM auction_listings WHERE id = $1`, id,
	).Scan(&l.ID, &l.SellerRoleID, &l.ItemID, &l.Qty, &l.Price, &l.Status, &l.BuyerRoleID, &l.CreatedAt, &l.RequestKey)
	return l, err
}

// ListOpen 列出在售挂牌，按 id 降序。
func (r *AuctionRepo) ListOpen(ctx context.Context, limit int) ([]AuctionListing, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, seller_role_id, item_id, qty, price, status, COALESCE(buyer_role_id, 0), created_at,
		        COALESCE(request_key, '')
		 FROM auction_listings WHERE status = $1 ORDER BY id DESC LIMIT $2`, AuctionOpen, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuctionListing
	for rows.Next() {
		var l AuctionListing
		if err := rows.Scan(&l.ID, &l.SellerRoleID, &l.ItemID, &l.Qty, &l.Price, &l.Status, &l.BuyerRoleID, &l.CreatedAt, &l.RequestKey); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// MarkSold 乐观锁式标记售出：仅 status=Open 时更新为 Sold 并记录 buyer。
func (r *AuctionRepo) MarkSold(ctx context.Context, id, buyerRoleID int64) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE auction_listings SET status = $2, buyer_role_id = $3, updated_at = NOW()
		 WHERE id = $1 AND status IN ($4, $2) AND buyer_role_id = $3`,
		id, AuctionSold, buyerRoleID, AuctionReserved,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
