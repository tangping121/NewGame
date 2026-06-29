package guildwar

import (
	"context"
	"fmt"
	"strconv"

	"newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
)

const (
	scoreKey  = "ng:guildwar:score"  // 全服公会战积分 ZSET，member=guildID
	seasonKey = "ng:guildwar:season" // 当前赛季序号 STRING
)

// ScoreEntry 单公会积分条目。
type ScoreEntry struct {
	GuildID int64   `json:"guild_id"` // 公会 ID
	Score   float64 `json:"score"`    // 累计积分（Redis ZSCORE）
}

// State 公会战当前赛季状态与 TOP 榜。
type State struct {
	Season int64        `json:"season"` // 赛季编号，从 1 递增
	Top    []ScoreEntry `json:"top"`    // 积分前 10 名公会
}

// Service 公会战逻辑：Redis 跨服积分榜，无 Redis 时内存兜底。
type Service struct {
	redis  goredis.UniversalClient // Redis；nil 时用 mem
	mem    map[int64]int64 // 内存模式：guildID -> score
	season int64           // 内存模式当前赛季
}

// New 创建公会战服务。
//
// 参数:
//   - rdb: Redis 客户端；nil 为纯内存模式（单进程测试）
func New(rdb goredis.UniversalClient) *Service {
	return &Service{redis: rdb, mem: map[int64]int64{}}
}

// State 查询当前赛季号与积分 TOP10。
//
// 参数:
//   - ctx: Redis 读上下文
func (s *Service) State(ctx context.Context) (State, error) {
	st := State{Season: 1}
	if s.redis == nil {
		if s.season > 0 {
			st.Season = s.season
		}
		for gid, sc := range s.mem {
			st.Top = append(st.Top, ScoreEntry{GuildID: gid, Score: float64(sc)})
		}
		return st, nil
	}
	if v, err := s.redis.Get(ctx, seasonKey).Int64(); err == nil {
		st.Season = v
	}
	vals, err := s.redis.ZRevRangeWithScores(ctx, scoreKey, 0, 9).Result()
	if err != nil {
		return st, err
	}
	for _, z := range vals {
		gid, _ := strconv.ParseInt(fmt.Sprint(z.Member), 10, 64)
		st.Top = append(st.Top, ScoreEntry{GuildID: gid, Score: z.Score})
	}
	return st, nil
}

// Attack 为公会增加战斗积分。
//
// 参数:
//   - ctx: Redis 写上下文
//   - guildID: 进攻方公会 ID，必须 > 0
//   - damage: 本次贡献积分；<=0 时按 100 计
//
// 返回: 更新后的 State
func (s *Service) Attack(ctx context.Context, guildID, damage int64) (State, error) {
	if guildID <= 0 {
		return State{}, fmt.Errorf("guild_id required")
	}
	if damage <= 0 {
		damage = 100
	}
	if s.redis == nil {
		if s.season == 0 {
			s.season = 1
		}
		s.mem[guildID] += damage
		return s.State(ctx)
	}
	if err := s.redis.SetNX(ctx, seasonKey, 1, 0).Err(); err != nil {
		redis.RecordError("guildwar", "season_init")
	}
	if err := s.redis.ZIncrBy(ctx, scoreKey, float64(damage), strconv.FormatInt(guildID, 10)).Err(); err != nil {
		redis.RecordError("guildwar", "zincr")
		return State{}, err
	}
	return s.State(ctx)
}

// ResetSeason 结束当前赛季：清空积分榜并 season+1。
//
// 参数:
//   - ctx: Redis 上下文
//
// 返回: 新赛季 State（Top 为空）
func (s *Service) ResetSeason(ctx context.Context) (State, error) {
	if s.redis == nil {
		if s.season == 0 {
			s.season = 1
		}
		s.season++
		s.mem = map[int64]int64{}
		return State{Season: s.season}, nil
	}
	pipe := s.redis.Pipeline()
	incr := pipe.Incr(ctx, seasonKey)
	pipe.Del(ctx, scoreKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return State{}, err
	}
	season, err := incr.Result()
	if err != nil {
		return State{}, err
	}
	return State{Season: season}, nil
}
