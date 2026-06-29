// Package gateforward Gate 向 Game 转发玩家消息，支持 HTTP 连接池与 gRPC 两种传输。
package gateforward

import "context"

// Forwarder Gate→Game 转发器统一抽象。
//
// target 含义随传输而定：
//   - HTTP：Game 的 HTTP 基址，如 http://127.0.0.1:9100
//   - gRPC：Game 的 gRPC host:port，如 127.0.0.1:9200
type Forwarder interface {
	// Forward 转发单帧玩家消息到 target 指向的 Game 分片，返回响应 payload。
	Forward(ctx context.Context, target string, roleID int64, zoneID int32, cmd, act uint16, body []byte) ([]byte, error)
}
