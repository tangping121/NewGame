// Package rpccodec 提供 gRPC 的 JSON 编解码器，使服务无需 protoc 生成代码即可通信。
//
// 仅用于内部 RPC（Gate↔Game 转发）。生产可替换为 protobuf 以获得更高性能，
// 但 JSON 编解码保持与现有 TCP/HTTP 协议一致的 payload 语义，便于平滑演进。
package rpccodec

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// Name 编解码器名称，gRPC 通过 content-subtype 协商使用。
const Name = "json"

// JSON 实现 google.golang.org/grpc/encoding.Codec。
type JSON struct{}

// Marshal 序列化消息为 JSON 字节。
func (JSON) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal 将 JSON 字节反序列化到消息。
func (JSON) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Name 返回编解码器名称。
func (JSON) Name() string {
	return Name
}

func init() {
	encoding.RegisterCodec(JSON{})
}
