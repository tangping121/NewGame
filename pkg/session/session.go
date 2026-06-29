// Package session 在 Redis 中维护登录 token 与角色/区服的绑定关系，供 Gate 鉴权使用。
package session

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const keyPrefix = "session:"

// Info 写入 Redis 的会话内容，Gate CmdLogin 成功后据此绑定 role_id。
type Info struct {
	RoleID int64 `json:"role_id"` // 角色唯一 ID
	ZoneID int32 `json:"zone_id"` // 角色所属区服 ID
}

func key(token string) string {
	return keyPrefix + token
}

// Save 将登录会话持久化到 Redis。
//
// 参数:
//   - ctx: 上下文，控制 Redis 操作超时
//   - rdb: Redis 客户端
//   - token: Login 返回给客户端的会话令牌，如 tk_xxx
//   - info: 绑定的角色 ID 与区服 ID
//   - ttl: 过期时间；<=0 时默认 24 小时
//
// 返回: Redis 写入或 JSON 序列化失败时的错误
func Save(ctx context.Context, rdb goredis.UniversalClient, token string, info Info, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, key(token), b, ttl).Err()
}

// Load 根据 token 读取会话。
//
// 参数:
//   - ctx: 上下文
//   - rdb: Redis 客户端
//   - token: 客户端携带的会话令牌
//
// 返回:
//   - Info: 会话中的角色与区服信息
//   - error: token 不存在、已过期或 role_id 无效时返回错误
func Load(ctx context.Context, rdb goredis.UniversalClient, token string) (Info, error) {
	raw, err := rdb.Get(ctx, key(token)).Result()
	if err == goredis.Nil {
		return Info{}, fmt.Errorf("session not found")
	}
	if err != nil {
		return Info{}, err
	}
	var info Info
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		return Info{}, err
	}
	if info.RoleID == 0 {
		return Info{}, fmt.Errorf("invalid session")
	}
	return info, nil
}
