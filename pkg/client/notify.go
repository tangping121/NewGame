package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"newgame/pkg/internalauth"
	"newgame/pkg/presence"

	goredis "github.com/redis/go-redis/v9"
)

// NotifyClient 向在线玩家推送服务端消息。
//
// 流程：presence 查玩家所在 Gate 的 HTTP 地址 → POST {gate}/internal/push
// → 该 Gate 将帧通过 TCP 写给客户端。支持跨分片、跨 Gate 通知。
type NotifyClient struct {
	redis  goredis.UniversalClient
	http   *http.Client
	secret string // 内部接口鉴权密钥
}

// NewNotifyClient 创建在线推送客户端。
//
// 参数:
//   - rdb: Redis 客户端，用于读取 presence
func NewNotifyClient(rdb goredis.UniversalClient) *NotifyClient {
	return &NotifyClient{
		redis: rdb,
		http:  &http.Client{Timeout: 2 * time.Second},
	}
}

// WithSecret 设置内部接口鉴权密钥（链式）。
func (c *NotifyClient) WithSecret(secret string) *NotifyClient {
	c.secret = secret
	return c
}

// ErrOffline 玩家不在线（presence 无记录）。
var ErrOffline = fmt.Errorf("player offline")

// Push 向指定玩家推送一帧。
//
// 参数:
//   - ctx: 请求上下文
//   - roleID: 目标角色 ID
//   - cmd, act: 协议字，通常 cmd=protocol.CmdPush
//   - body: JSON payload
//
// 返回:
//   - ErrOffline: 玩家不在线
//   - 其他 error: presence 读取或 Gate 调用失败
func (c *NotifyClient) Push(ctx context.Context, roleID int64, cmd, act uint16, body []byte) error {
	if c.redis == nil {
		return ErrOffline
	}
	rec, err := presence.Load(ctx, c.redis, roleID)
	if err != nil {
		return ErrOffline
	}
	if rec.GateHTTP == "" {
		return fmt.Errorf("presence missing gate_http for role %d", roleID)
	}
	payload, _ := json.Marshal(map[string]any{
		"role_id": roleID,
		"cmd":     cmd,
		"act":     act,
		"body":    string(body),
	})
	url := "http://" + rec.GateHTTP + "/internal/push"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
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
		return fmt.Errorf("gate push status %d", resp.StatusCode)
	}
	var out struct {
		Code int `json:"code"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Code != 0 {
		return ErrOffline
	}
	return nil
}
