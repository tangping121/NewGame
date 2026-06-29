// Package gateforward Gate 向 Game 转发玩家消息，支持 HTTP 连接池（P1 演进）。
package gateforward

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"newgame/pkg/internalauth"
	"newgame/pkg/protocol"
	"newgame/pkg/scale"
)

// HTTPPool 复用 TCP 连接转发至 Game /internal/player/msg。
type HTTPPool struct {
	client *http.Client
	secret string // 内部接口鉴权密钥
}

// SetSecret 设置内部接口鉴权密钥。
func (p *HTTPPool) SetSecret(s string) { p.secret = s }

// NewHTTPPool 创建带连接池的 HTTP 转发器。
//
// 参数:
//   - poolSize: 每 host 最大空闲连接数；<=0 时使用 scale.GateGamePoolSize
func NewHTTPPool(poolSize int) *HTTPPool {
	if poolSize <= 0 {
		poolSize = scale.GateGamePoolSize
	}
	return &HTTPPool{
		client: &http.Client{
			Timeout: scale.GateHTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        poolSize * 4,
				MaxIdleConnsPerHost: poolSize,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Forward 将 CmdGame 帧 POST 到 target/internal/player/msg（target 为 HTTP 基址）。
//
// cmd/act/role 走请求头，原始 payload 直接作为 HTTP body，避免「JSON 套 JSON」双重编码。
// zoneID 当前未用于 HTTP 路由（由调用方在选址时决定），保留以满足 Forwarder 接口。
func (p *HTTPPool) Forward(ctx context.Context, target string, roleID int64, zoneID int32, cmd, act uint16, body []byte) ([]byte, error) {
	_ = zoneID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/internal/player/msg", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(protocol.HeaderCmd, strconv.FormatUint(uint64(cmd), 10))
	req.Header.Set(protocol.HeaderAct, strconv.FormatUint(uint64(act), 10))
	req.Header.Set(protocol.HeaderRole, strconv.FormatInt(roleID, 10))
	internalauth.SetHTTP(req, p.secret)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
