package worldboss

import (
	"context"
	"fmt"
	"strconv"

	"newgame/pkg/redis"

	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultHP = 100000
	hpKey     = "ng:worldboss:hp"     // 全服共享 Boss 剩余血量 STRING
	damageKey = "ng:worldboss:damage" // 跨区伤害榜 ZSET，member=z{zone}:{role}
)

// attackScript 原子结算一次攻击：初始化 HP、按剩余血量限伤（不超杀）、
// 扣血并只按有效伤害累计到伤害榜，返回扣血后的 HP。
// KEYS[1]=hpKey KEYS[2]=damageKey；ARGV[1]=maxHP ARGV[2]=damage ARGV[3]=member。
var attackScript = goredis.NewScript(`
local hp = redis.call('GET', KEYS[1])
if not hp then
  hp = tonumber(ARGV[1])
else
  hp = tonumber(hp)
end
local dmg = tonumber(ARGV[2])
if dmg > hp then dmg = hp end
if dmg < 0 then dmg = 0 end
local newhp = hp - dmg
redis.call('SET', KEYS[1], newhp)
if dmg > 0 then
  redis.call('ZINCRBY', KEYS[2], dmg, ARGV[3])
end
return newhp
`)

// Service 世界 Boss：全服共享 HP 与伤害排名。
type Service struct {
	redis goredis.UniversalClient // Redis；nil 时 Attack 返回空状态
	maxHP int64           // Boss 最大血量
}

// New 创建世界 Boss 服务。
//
// 参数:
//   - rdb: Redis 客户端
func New(rdb goredis.UniversalClient) *Service {
	return &Service{redis: rdb, maxHP: defaultHP}
}

// DamageEntry 伤害榜单条记录。
type DamageEntry struct {
	Role string  `json:"role"` // 成员键，如 z1:10001
	Dmg  float64 `json:"damage"` // 累计伤害
}

// State Boss 当前血量与 TOP5 伤害。
type State struct {
	HP        int64         `json:"hp"`         // 当前血量
	MaxHP     int64         `json:"max_hp"`     // 最大血量
	TopDamage []DamageEntry `json:"top_damage"` // 伤害前 5
}

// State 查询 Boss 血量与伤害榜。
func (s *Service) State(ctx context.Context) (State, error) {
	st := State{MaxHP: s.maxHP}
	if s.redis == nil {
		st.HP = s.maxHP
		return st, nil
	}
	v, err := s.redis.Get(ctx, hpKey).Result()
	if err == goredis.Nil {
		if err := s.redis.SetNX(ctx, hpKey, s.maxHP, 0).Err(); err != nil {
			redis.RecordError("worldboss", "hp_init")
		}
		st.HP = s.maxHP
	} else if err != nil {
		return st, err
	} else {
		hp, _ := strconv.ParseInt(v, 10, 64)
		if hp < 0 {
			hp = 0
		}
		st.HP = hp
	}
	vals, _ := s.redis.ZRevRangeWithScores(ctx, damageKey, 0, 4).Result()
	for _, z := range vals {
		st.TopDamage = append(st.TopDamage, DamageEntry{Role: fmt.Sprint(z.Member), Dmg: z.Score})
	}
	return st, nil
}

// Attack 玩家对 Boss 造成伤害并更新伤害榜。
//
// 参数:
//   - ctx: Redis 上下文
//   - roleID: 攻击者角色 ID
//   - zoneID: 攻击者区服 ID（用于跨区榜 member 编码）
//   - damage: 伤害值；<=0 时按 100 计
func (s *Service) Attack(ctx context.Context, roleID int64, zoneID int32, damage int64) (State, error) {
	if damage <= 0 {
		damage = 100
	}
	if s.redis == nil {
		return State{HP: 0, MaxHP: s.maxHP}, nil
	}
	member := fmt.Sprintf("z%d:%d", zoneID, roleID)
	// 原子结算，避免并发下「超杀伤害计入榜单」与「HP 读到负数」。
	if err := attackScript.Run(ctx, s.redis,
		[]string{hpKey, damageKey},
		s.maxHP, damage, member,
	).Err(); err != nil {
		return State{}, err
	}
	return s.State(ctx)
}

// Reset 重置 Boss 满血并清空伤害榜（赛季/活动结束调用）。
func (s *Service) Reset(ctx context.Context) error {
	if s.redis == nil {
		return nil
	}
	pipe := s.redis.Pipeline()
	pipe.Set(ctx, hpKey, s.maxHP, 0)
	pipe.Del(ctx, damageKey)
	_, err := pipe.Exec(ctx)
	return err
}
