package gateforward

import (
	"context"
	"fmt"
	"sync"
	"time"

	"newgame/pkg/gamerpc"
	"newgame/pkg/internalauth"
	"newgame/pkg/rpccodec"
	"newgame/pkg/scale"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCPool 维护到各 Game 分片的 gRPC 长连接，多路复用玩家消息。
//
// 相比 HTTP 连接池，gRPC 基于 HTTP/2 单连接多路复用，减少握手与队头阻塞，
// 是 50 万～100 万 CCU 阶段 Gate→Game 的推荐传输（见 docs/architecture-scale.md）。
type GRPCPool struct {
	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn // grpc host:port -> 连接
	timeout time.Duration
	secret  string // 内部接口鉴权密钥
}

// NewGRPCPool 创建 gRPC 转发池。
func NewGRPCPool() *GRPCPool {
	return &GRPCPool{
		conns:   make(map[string]*grpc.ClientConn),
		timeout: scale.GateHTTPTimeout,
	}
}

// SetSecret 设置内部接口鉴权密钥。
func (p *GRPCPool) SetSecret(s string) { p.secret = s }

// Forward 通过 gRPC 调用 target（Game gRPC 地址）的 Forward 方法。
func (p *GRPCPool) Forward(ctx context.Context, target string, roleID int64, zoneID int32, cmd, act uint16, body []byte) ([]byte, error) {
	if target == "" {
		return nil, fmt.Errorf("empty grpc target")
	}
	cc, err := p.conn(target)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	cctx = internalauth.WithOutgoing(cctx, p.secret)
	resp, err := gamerpc.NewForwarderClient(cc).Forward(cctx, &gamerpc.ForwardRequest{
		RoleID: roleID,
		ZoneID: zoneID,
		Cmd:    cmd,
		Act:    act,
		Body:   body,
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// conn 获取或懒建到 target 的 gRPC 连接。
func (p *GRPCPool) conn(target string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	cc := p.conns[target]
	p.mu.RUnlock()
	if cc != nil {
		return cc, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cc = p.conns[target]; cc != nil {
		return cc, nil
	}
	cc, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpccodec.JSON{})),
	)
	if err != nil {
		return nil, err
	}
	p.conns[target] = cc
	return cc, nil
}

// Close 关闭所有 gRPC 连接。
func (p *GRPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cc := range p.conns {
		_ = cc.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
}
