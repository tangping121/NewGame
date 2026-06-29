// Package gamerpc 定义 Gate↔Game 的 gRPC 转发服务（手写 ServiceDesc，免 protoc）。
//
// 服务: newgame.GameForwarder
//   - Forward(ForwardRequest) ForwardResponse  一元调用，转发单帧玩家消息
//
// 消息使用 JSON 编解码（见 pkg/rpccodec），payload 语义与 TCP/HTTP 协议一致。
package gamerpc

import (
	"context"

	"newgame/pkg/rpccodec"

	"google.golang.org/grpc"
)

// FullMethodForward Forward 方法的全名。
const FullMethodForward = "/newgame.GameForwarder/Forward"

// ForwardRequest Gate 转发到 Game 的玩家消息。
type ForwardRequest struct {
	RoleID int64  `json:"role_id"` // 已鉴权角色 ID（同时决定分片）
	ZoneID int32  `json:"zone_id"` // 区服
	Cmd    uint16 `json:"cmd"`     // 协议命令字，通常 CmdGame
	Act    uint16 `json:"act"`     // 协议动作字
	Body   []byte `json:"body"`    // 原始 payload（JSON 字节）
}

// ForwardResponse Game 处理后的响应。
type ForwardResponse struct {
	Body []byte `json:"body"` // 响应 payload（JSON 字节）
}

// ForwarderServer Game 侧需实现的接口。
type ForwarderServer interface {
	Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error)
}

// ForwarderClient Gate 侧调用接口。
type ForwarderClient interface {
	Forward(ctx context.Context, req *ForwardRequest, opts ...grpc.CallOption) (*ForwardResponse, error)
}

// ServiceDesc 手写的 gRPC 服务描述，等价于 protoc 生成的 _grpc.pb.go。
var ServiceDesc = grpc.ServiceDesc{
	ServiceName: "newgame.GameForwarder",
	HandlerType: (*ForwarderServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Forward",
			Handler:    forwardHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "gamerpc",
}

// RegisterForwarderServer 在 gRPC Server 上注册实现。
func RegisterForwarderServer(s *grpc.Server, impl ForwarderServer) {
	s.RegisterService(&ServiceDesc, impl)
}

func forwardHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ForwardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ForwarderServer).Forward(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: FullMethodForward}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ForwarderServer).Forward(ctx, req.(*ForwardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type forwarderClient struct {
	cc grpc.ClientConnInterface
}

// NewForwarderClient 基于已建立的连接创建客户端。
func NewForwarderClient(cc grpc.ClientConnInterface) ForwarderClient {
	return &forwarderClient{cc: cc}
}

func (c *forwarderClient) Forward(ctx context.Context, req *ForwardRequest, opts ...grpc.CallOption) (*ForwardResponse, error) {
	out := new(ForwardResponse)
	// 强制使用 JSON 编解码器，与服务端协商一致。
	opts = append([]grpc.CallOption{grpc.ForceCodec(rpccodec.JSON{})}, opts...)
	if err := c.cc.Invoke(ctx, FullMethodForward, req, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}
