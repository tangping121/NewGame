// Package auction 拍卖行：上架、购买、列表；扣背包/金币并过户。
package auction

import (
	"context"
	"fmt"

	"newgame/pkg/repo"
	"newgame/services/game/internal/player"
)

// Listing 对外展示的拍卖条目（在售）。
type Listing struct {
	ID           int64  `json:"id"`             // 挂牌 ID
	SellerRoleID int64  `json:"seller_role_id"` // 卖家角色 ID
	ItemID       string `json:"item_id"`        // 道具 ID
	Qty          int32  `json:"qty"`            // 数量
	Price        int64  `json:"price"`          // 总价（金币）
}

// Service 拍卖行业务；有 Postgres 时持久化，否则 mem 内存列表。
type Service struct {
	repo    *repo.AuctionRepo // 拍卖表；nil 用 mem
	players *player.Manager   // 玩家 Actor，用于扣/add 物品与金币
	mem     []Listing         // 内存模式挂牌列表
	nextID  int64             // 内存模式自增 ID
}

// New 创建拍卖服务。
//
// 参数:
//   - r: 拍卖仓库；可为 nil
//   - players: 玩家管理器，不可为 nil
func New(r *repo.AuctionRepo, players *player.Manager) *Service {
	return &Service{repo: r, players: players, mem: []Listing{}}
}

// List 列出在售挂牌。
//
// 参数:
//   - ctx: 查询上下文
//   - limit: 最大条数；repo 内部 <=0 时默认 50
func (s *Service) List(ctx context.Context, limit int) ([]Listing, error) {
	if s.repo != nil {
		rows, err := s.repo.ListOpen(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]Listing, 0, len(rows))
		for _, r := range rows {
			out = append(out, Listing{
				ID: r.ID, SellerRoleID: r.SellerRoleID, ItemID: r.ItemID, Qty: r.Qty, Price: r.Price,
			})
		}
		return out, nil
	}
	return append([]Listing(nil), s.mem...), nil
}

// Create 卖家上架：扣背包物品，写入挂牌。
//
// 参数:
//   - ctx: 上下文
//   - sellerRoleID: 卖家角色 ID
//   - itemID: 道具 ID
//   - qty: 上架数量，必须 > 0
//   - price: 售价（金币），必须 > 0
//
// 返回: 新挂牌；背包不足或 DB 失败时 error（失败会回滚背包）
func (s *Service) Create(ctx context.Context, sellerRoleID int64, itemID string, qty int32, price int64) (Listing, error) {
	if itemID == "" || qty <= 0 || price <= 0 {
		return Listing{}, fmt.Errorf("invalid listing")
	}
	pl := s.players.Get(ctx, sellerRoleID)
	if !pl.Inv.Remove(itemID, qty) {
		return Listing{}, fmt.Errorf("insufficient items")
	}
	if err := s.players.SaveNow(ctx, sellerRoleID); err != nil {
		return Listing{}, err
	}
	if s.repo != nil {
		id, err := s.repo.Create(ctx, repo.AuctionListing{
			SellerRoleID: sellerRoleID, ItemID: itemID, Qty: qty, Price: price,
		})
		if err != nil {
			pl.Inv.Add(itemID, qty)
			_ = s.players.SaveNow(ctx, sellerRoleID)
			return Listing{}, err
		}
		return Listing{ID: id, SellerRoleID: sellerRoleID, ItemID: itemID, Qty: qty, Price: price}, nil
	}
	s.nextID++
	l := Listing{ID: s.nextID, SellerRoleID: sellerRoleID, ItemID: itemID, Qty: qty, Price: price}
	s.mem = append(s.mem, l)
	return l, nil
}

// Buy 买家购买挂牌：扣金币、加物品，卖家收金币。
//
// 参数:
//   - ctx: 上下文
//   - buyerRoleID: 买家角色 ID
//   - listingID: 挂牌 ID
//
// 返回: 成交的 Listing；不能买自己的、金币不足、已售出时 error
func (s *Service) Buy(ctx context.Context, buyerRoleID, listingID int64) (Listing, error) {
	buyer := s.players.Get(ctx, buyerRoleID)
	if s.repo != nil {
		l, err := s.repo.Get(ctx, listingID)
		if err != nil {
			return Listing{}, fmt.Errorf("listing not found")
		}
		if l.Status != repo.AuctionOpen {
			return Listing{}, fmt.Errorf("listing not available")
		}
		if l.SellerRoleID == buyerRoleID {
			return Listing{}, fmt.Errorf("cannot buy own listing")
		}
		if !buyer.SpendGold(l.Price) {
			return Listing{}, fmt.Errorf("insufficient gold")
		}
		buyer.Inv.Add(l.ItemID, l.Qty)
		if err := s.players.SaveNow(ctx, buyerRoleID); err != nil {
			buyer.AddGold(l.Price)
			buyer.Inv.Remove(l.ItemID, l.Qty)
			_ = s.players.SaveNow(ctx, buyerRoleID)
			return Listing{}, err
		}
		if err := s.repo.MarkSold(ctx, listingID, buyerRoleID); err != nil {
			buyer.AddGold(l.Price)
			buyer.Inv.Remove(l.ItemID, l.Qty)
			_ = s.players.SaveNow(ctx, buyerRoleID)
			return Listing{}, fmt.Errorf("listing not available")
		}
		seller := s.players.Get(ctx, l.SellerRoleID)
		seller.AddGold(l.Price)
		_ = s.players.SaveNow(ctx, l.SellerRoleID)
		return Listing{ID: l.ID, SellerRoleID: l.SellerRoleID, ItemID: l.ItemID, Qty: l.Qty, Price: l.Price}, nil
	}
	for i, l := range s.mem {
		if l.ID != listingID {
			continue
		}
		if l.SellerRoleID == buyerRoleID {
			return Listing{}, fmt.Errorf("cannot buy own listing")
		}
		if !buyer.SpendGold(l.Price) {
			return Listing{}, fmt.Errorf("insufficient gold")
		}
		buyer.Inv.Add(l.ItemID, l.Qty)
		_ = s.players.SaveNow(ctx, buyerRoleID)
		seller := s.players.Get(ctx, l.SellerRoleID)
		seller.AddGold(l.Price)
		_ = s.players.SaveNow(ctx, l.SellerRoleID)
		s.mem = append(s.mem[:i], s.mem[i+1:]...)
		return l, nil
	}
	return Listing{}, fmt.Errorf("listing not found")
}
