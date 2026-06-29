package dungeon

import (
	"context"
	"encoding/json"

	"newgame/api/pb"
	"newgame/pkg/mq"
	"newgame/services/game/internal/player"

	"github.com/nats-io/nats.go"
)

const goldPerPass int64 = 50

// Service 副本通关：升级、发金币，并通过 NATS 更新排行榜。
type Service struct {
	zoneID int32
	nats   *nats.Conn
}

func New(zoneID int32, nc *nats.Conn) *Service {
	return &Service{zoneID: zoneID, nats: nc}
}

type PassResult struct {
	Level int32 `json:"level"`
	Gold  int64 `json:"gold"`
}

func (s *Service) Pass(ctx context.Context, p *player.Actor, dungeonID int32) (PassResult, error) {
	_ = ctx
	_ = dungeonID
	p.LevelUp()
	p.AddGold(goldPerPass)
	p.Inv.Add("potion", 1)
	if s.nats != nil {
		msg, _ := json.Marshal(pb.RankUpdateRequest{
			ZoneId: s.zoneID,
			RoleId: p.ID,
			Score:  int64(p.Level),
			Board:  "dungeon",
		})
		_ = s.nats.Publish(mq.SubjectRankUpdate, msg)
	}
	return PassResult{Level: p.Level, Gold: p.Gold}, nil
}
