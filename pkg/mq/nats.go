// Package mq 封装 NATS 连接；主题名与载荷定义见 pkg/protocol/nats.go。
package mq

import (
	"github.com/nats-io/nats.go"

	"newgame/pkg/protocol"
)

// Connect 连接 NATS 服务器。
//
// 参数:
//   - url: 如 nats://127.0.0.1:4222
func Connect(url string) (*nats.Conn, error) {
	return nats.Connect(url)
}

// NATS 主题常量（与 pkg/protocol 保持一致，便于 mq 包引用）。
const (
	SubjectMailSend    = protocol.SubjectMailSend
	SubjectRankUpdate  = protocol.SubjectRankUpdate
	SubjectActivityEvt = protocol.SubjectActivityEvent
)
