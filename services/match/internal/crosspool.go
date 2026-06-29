package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const poolKeyPrefix = "ng:match:pool:"

// joinScript Redis Lua：原子入队并尝试凑满匹配。
// KEYS[1]=队列键；ARGV[1]=成员 z{zone}:{role}；ARGV[2]=所需人数 need。
// 逻辑：LREM 去重 -> RPUSH 入队 -> LLEN>=need 则 LPOP 弹出 need 人并返回。
var joinScript = goredis.NewScript(`
local key = KEYS[1]
local member = ARGV[1]
local need = tonumber(ARGV[2])
redis.call('LREM', key, 0, member)
redis.call('RPUSH', key, member)
local len = redis.call('LLEN', key)
if len >= need then
  local out = {}
  for i = 1, need do
    out[i] = redis.call('LPOP', key)
  end
  return out
end
return {}
`)

// QueueEntry 匹配队列中的单个玩家。
type QueueEntry struct {
	ZoneID int32  // 区服 ID
	RoleID int64  // 角色 ID
	Raw    string // Redis 中原始成员串，如 z1:10001
}

// CrossPool 基于 Redis List 的跨服匹配等待队列。
type CrossPool struct {
	rdb goredis.UniversalClient // Redis 客户端，nil 时 Join 报错
}

// NewCrossPool 创建跨服匹配池。
//
// 参数:
//   - rdb: Redis 客户端
func NewCrossPool(rdb goredis.UniversalClient) *CrossPool {
	return &CrossPool{rdb: rdb}
}

func poolKey(mode int32) string {
	return fmt.Sprintf("%s%d", poolKeyPrefix, mode)
}

// MemberKey 将区服与角色编码为队列成员字符串。
//
// 参数:
//   - zoneID: 区服 ID
//   - roleID: 角色 ID
//
// 返回: 如 "z2:10002"
func MemberKey(zoneID int32, roleID int64) string {
	return fmt.Sprintf("z%d:%d", zoneID, roleID)
}

// ParseMember 解析 MemberKey 生成的字符串。
//
// 参数:
//   - raw: 如 "z1:10001"
//
// 返回: QueueEntry；格式错误时 error
func ParseMember(raw string) (QueueEntry, error) {
	if !strings.HasPrefix(raw, "z") {
		return QueueEntry{}, fmt.Errorf("invalid member %q", raw)
	}
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return QueueEntry{}, fmt.Errorf("invalid member %q", raw)
	}
	zoneID, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "z"), 10, 32)
	if err != nil {
		return QueueEntry{}, err
	}
	roleID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return QueueEntry{}, err
	}
	return QueueEntry{ZoneID: int32(zoneID), RoleID: roleID, Raw: raw}, nil
}

// Join 玩家加入跨服匹配队列。
//
// 参数:
//   - ctx: Redis 脚本执行上下文
//   - mode: 匹配模式 ID，决定队列键 ng:match:pool:{mode}
//   - zoneID: 玩家区服
//   - roleID: 玩家角色 ID
//   - need: 匹配成功所需人数，通常为 2
//
// 返回:
//   - []QueueEntry: 凑满 need 人时返回被匹配的一组；否则 nil
//   - error: Redis 错误或 rdb 为 nil
func (p *CrossPool) Join(ctx context.Context, mode int32, zoneID int32, roleID int64, need int) ([]QueueEntry, error) {
	if p.rdb == nil {
		return nil, fmt.Errorf("redis required for cross-zone match")
	}
	member := MemberKey(zoneID, roleID)
	res, err := joinScript.Run(ctx, p.rdb, []string{poolKey(mode)}, member, need).StringSlice()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	out := make([]QueueEntry, 0, len(res))
	for _, raw := range res {
		e, err := ParseMember(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// QueueSize 查询指定模式队列中等待人数。
//
// 参数:
//   - ctx: Redis 上下文
//   - mode: 匹配模式 ID
func (p *CrossPool) QueueSize(ctx context.Context, mode int32) (int64, error) {
	if p.rdb == nil {
		return 0, nil
	}
	return p.rdb.LLen(ctx, poolKey(mode)).Result()
}
