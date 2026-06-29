// Package client 封装对其他微服务内部 HTTP API 的调用，统一服务发现与超时。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"newgame/pkg/discovery"
	"newgame/pkg/grant"
	"newgame/pkg/internalauth"
	"newgame/pkg/shard"
)

// GameClient 调用 Game 服务 /internal/* 接口的 HTTP 客户端。
//
// 分片模式（shardCount>0）：按 role_id 路由到 game-shard-{role_id%N}，
// 与 Gate 的路由保持一致，保证同一玩家的请求总落到同一 Game 分片。
type GameClient struct {
	disc       *discovery.Registry  // 服务发现；nil 时使用硬编码回退地址
	resolver   *discovery.Resolver  // 带 TTL 缓存的实例解析，避免每次健康探测
	zoneID     int32                // 目标区服
	shardCount int32                // Game 分片总数；<=0 表示单进程（注册名 "game"）
	secret     string               // 内部接口鉴权密钥
	http       *http.Client         // HTTP 客户端，超时 5 秒
}

// NewGameClient 创建 Game 内部 API 客户端（单进程模式）。
//
// 参数:
//   - disc: Redis 服务发现注册表；可为 nil
//   - zoneID: 角色所在区服 ID
func NewGameClient(disc *discovery.Registry, zoneID int32) *GameClient {
	return NewGameClientSharded(disc, zoneID, 0)
}

// NewGameClientSharded 创建支持分片路由的 Game 内部 API 客户端。
//
// 参数:
//   - disc: Redis 服务发现注册表；可为 nil
//   - zoneID: 角色所在区服 ID
//   - shardCount: Game 分片总数；<=0 时退化为单进程 "game"
func NewGameClientSharded(disc *discovery.Registry, zoneID int32, shardCount int32) *GameClient {
	var resolver *discovery.Resolver
	if disc != nil {
		resolver = discovery.NewResolver(disc, 2*time.Second)
	}
	return &GameClient{
		disc:       disc,
		resolver:   resolver,
		zoneID:     zoneID,
		shardCount: shardCount,
		http:       &http.Client{Timeout: 5 * time.Second},
	}
}

// WithSecret 设置内部接口鉴权密钥（链式）。
func (c *GameClient) WithSecret(secret string) *GameClient {
	c.secret = secret
	return c
}

// baseURLForRole 按 role_id 解析目标 Game 实例 HTTP 基址。
//
// 分片模式：发现 game-shard-{role_id%N}；单进程：发现 "game"。
// 发现失败时按 zoneID 回退本地端口。
func (c *GameClient) baseURLForRole(ctx context.Context, roleID int64) string {
	if c.resolver != nil {
		name := "game"
		if c.shardCount > 0 {
			name = shard.ServiceName(shard.ForRole(roleID, c.shardCount))
		}
		if inst, ok := c.resolver.Resolve(ctx, name, c.zoneID); ok {
			return inst.HTTPBase()
		}
	}
	switch c.zoneID {
	case 2:
		return "http://127.0.0.1:9110"
	default:
		return "http://127.0.0.1:9100"
	}
}

// Grant 调用目标分片 /internal/player/grant 发放金币与道具。
//
// 参数:
//   - ctx: 请求上下文
//   - roleID: 接收奖励的角色 ID（同时决定分片路由）
//   - bundle: 金币与道具包
//   - source: 来源标识，用于日志/审计，如 "mail:123"、"pay:ord_xxx"
//
// 返回: HTTP 失败或响应 code!=0 时返回 error
func (c *GameClient) Grant(ctx context.Context, roleID int64, bundle grant.Bundle, source string) error {
	base := c.baseURLForRole(ctx, roleID)
	body, _ := json.Marshal(map[string]any{
		"role_id": roleID,
		"gold":    bundle.Gold,
		"items":   bundle.Items,
		"source":  source,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/player/grant", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	internalauth.SetHTTP(req, c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("game grant status %d", resp.StatusCode)
	}
	var out struct {
		Code int `json:"code"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Code != 0 {
		return fmt.Errorf("game grant code %d", out.Code)
	}
	return nil
}

// Logout 通知 role 所在 Game 分片卸载该玩家 Actor（Gate 断线时调用）。
//
// 失败不影响主流程（Game 侧有空闲淘汰兜底），仅尽力而为。
func (c *GameClient) Logout(ctx context.Context, roleID int64) error {
	base := c.baseURLForRole(ctx, roleID)
	body, _ := json.Marshal(map[string]any{"role_id": roleID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/player/logout", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	internalauth.SetHTTP(req, c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("game logout status %d", resp.StatusCode)
	}
	return nil
}

// GrantItems 先 Parse 奖励字符串，再调用 Grant。
//
// 参数:
//   - ctx: 请求上下文
//   - roleID: 角色 ID
//   - items: 奖励字符串，如 "gold:100,potion:2"
//   - source: 来源标识
func (c *GameClient) GrantItems(ctx context.Context, roleID int64, items string, source string) error {
	b, err := grant.Parse(items)
	if err != nil {
		return err
	}
	return c.Grant(ctx, roleID, b, source)
}
