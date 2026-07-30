package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const poolKeyPrefix = "ng:match:"

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

// CrossPool 基于 Redis List 的共享匹配等待队列。
// scope="global" 用于跨区匹配，scope="zone:N" 用于区内多副本共享队列；
// 两种模式复用相同原子入队算法，但绝不能共享同一个 Redis key。
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

// poolKey 将匹配范围和模式同时放入 Redis Cluster hash-tag。
// 同一队列的 Lua 操作因此位于一个 slot，不同区服或模式不会串队。
func poolKey(scope string, mode int32) string {
	return fmt.Sprintf("%s{%s:%d}:pool", poolKeyPrefix, scope, mode)
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

// Requeue 补偿房间创建失败，将成员放回原 scope/mode 队列。
// LREM 后再 RPUSH 保证重试不会让同一成员在等待队列中出现多次。
func (p *CrossPool) Requeue(ctx context.Context, scope string, mode int32, entries []QueueEntry) error {
	if p.rdb == nil {
		return fmt.Errorf("redis required for match compensation")
	}
	pipe := p.rdb.TxPipeline()
	key := poolKey(scope, mode)
	for _, entry := range entries {
		pipe.LRem(ctx, key, 0, entry.Raw)
		pipe.RPush(ctx, key, entry.Raw)
	}
	_, err := pipe.Exec(ctx)
	return err
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

// Join 将玩家加入指定 scope 和模式的共享匹配队列。
//
// 参数:
//   - ctx: Redis 脚本执行上下文
//   - scope: 匹配范围；global 表示跨区，zone:N 表示仅区服 N
//   - mode: 匹配模式 ID；与 scope 一起决定唯一队列
//   - zoneID: 玩家区服
//   - roleID: 玩家角色 ID
//   - need: 匹配成功所需人数，通常为 2
//
// 返回:
//   - []QueueEntry: 凑满 need 人时返回被匹配的一组；否则 nil
//   - error: Redis 错误或 rdb 为 nil
func (p *CrossPool) Join(
	ctx context.Context, scope string, mode int32, zoneID int32, roleID int64, need int,
) ([]QueueEntry, error) {
	if p.rdb == nil {
		return nil, fmt.Errorf("redis required for cross-zone match")
	}
	member := MemberKey(zoneID, roleID)
	res, err := joinScript.Run(ctx, p.rdb, []string{poolKey(scope, mode)}, member, need).StringSlice()
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

// QueueSize 查询指定 scope 和模式队列中的等待人数。
//
// 参数:
//   - ctx: Redis 上下文
//   - scope: global 或 zone:N
//   - mode: 匹配模式 ID
func (p *CrossPool) QueueSize(ctx context.Context, scope string, mode int32) (int64, error) {
	if p.rdb == nil {
		return 0, nil
	}
	return p.rdb.LLen(ctx, poolKey(scope, mode)).Result()
}
