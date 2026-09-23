// Package presence 维护玩家在线位置（Gate / Game 分片），供路由、踢人、跨服消息使用。
package presence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const keyPrefix = "ng:presence:"

var ErrStaleSession = errors.New("stale presence session")

// Record 玩家在线路由信息，写入 Redis Cluster。
type Record struct {
	RoleID    int64  `json:"role_id"`    // 角色 ID
	ZoneID    int32  `json:"zone_id"`    // 区服
	ShardID   int32  `json:"shard_id"`   // Game 分片
	GateID    string `json:"gate_id"`    // Gate 实例 ID（服务发现 inst.ID）
	GateAddr  string `json:"gate_addr"`  // Gate TCP 地址，可选
	GateHTTP  string `json:"gate_http"`  // Gate HTTP 地址，跨分片推送时调用 /internal/push
	SessionID string `json:"session_id"` // fencing generation for this connection
	LoginAt   int64  `json:"login_at"`   // 上线 Unix 秒
}

var storeScript = goredis.NewScript(`
local old = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return old or ''
`)

var removeScript = goredis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local value = cjson.decode(raw)
if value.session_id ~= ARGV[1] then return -1 end
return redis.call('DEL', KEYS[1])
`)

var renewScript = goredis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local value = cjson.decode(raw)
if value.session_id ~= ARGV[1] then return -1 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

func key(roleID int64) string {
	return keyPrefix + strconv.FormatInt(roleID, 10)
}

// Store 登记玩家在线，Gate CmdLogin 成功后调用。
//
// 参数:
//   - ctx: Redis 上下文
//   - rdb: Redis 客户端（生产环境用 Cluster）
//   - rec: 在线记录
//   - ttl: 过期时间；<=0 默认 24h，需 Gate 心跳续约
func Store(ctx context.Context, rdb goredis.UniversalClient, rec Record, ttl time.Duration) (Record, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if rec.LoginAt == 0 {
		rec.LoginAt = time.Now().Unix()
	}
	if rec.SessionID == "" {
		return Record{}, fmt.Errorf("presence session_id required")
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return Record{}, err
	}
	old, err := storeScript.Run(ctx, rdb, []string{key(rec.RoleID)}, b, ttl.Milliseconds()).Text()
	if err != nil {
		return Record{}, err
	}
	if old == "" {
		return Record{}, nil
	}
	var previous Record
	if err := json.Unmarshal([]byte(old), &previous); err != nil {
		return Record{}, err
	}
	return previous, nil
}

// Load 查询玩家是否在线及所在 Gate/分片。
func Load(ctx context.Context, rdb goredis.UniversalClient, roleID int64) (Record, error) {
	raw, err := rdb.Get(ctx, key(roleID)).Result()
	if err == goredis.Nil {
		return Record{}, fmt.Errorf("offline")
	}
	if err != nil {
		return Record{}, err
	}
	var rec Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// Remove 玩家下线时删除（Gate 连接断开）。
func Remove(ctx context.Context, rdb goredis.UniversalClient, roleID int64, sessionID string) error {
	result, err := removeScript.Run(ctx, rdb, []string{key(roleID)}, sessionID).Int()
	if err != nil {
		return err
	}
	if result < 0 {
		return ErrStaleSession
	}
	return nil
}

// Renew 心跳续约，防止长连接玩家 key 过期。
func Renew(ctx context.Context, rdb goredis.UniversalClient, roleID int64, sessionID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	result, err := renewScript.Run(ctx, rdb, []string{key(roleID)}, sessionID, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if result <= 0 {
		return ErrStaleSession
	}
	return nil
}

// ShardOnlineKey 某分片在线计数 Redis key（INCR/DECR 或 HyperLogLog 统计）。
func ShardOnlineKey(zoneID, shardID int32) string {
	return fmt.Sprintf("ng:online:z%d:shard:%d", zoneID, shardID)
}
