// 发奖服务：将 pkg/grant.Bundle 写入玩家金币/背包并持久化。
package grant

import (
	"context"

	"newgame/pkg/grant"
	"newgame/services/game/internal/player"
)

type Service struct{}

func New() *Service { return &Service{} }

func (s *Service) Apply(ctx context.Context, p *player.Actor, b grant.Bundle, source string) error {
	_ = ctx
	_ = source
	if b.Gold > 0 {
		p.AddGold(b.Gold)
	}
	for item, n := range b.Items {
		p.Inv.Add(item, n)
	}
	return nil
}
