package internal

import (
	"context"
	"fmt"
	"math"
	"net"

	"bastion/pkg/gamerpc"
	"bastion/pkg/internalauth"
	"bastion/pkg/internaltls"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Forward 实现 gamerpc.ForwarderServer：执行 Gate 经 gRPC 转发的玩家消息。
//
// 与 HTTP /internal/player/msg 等价，复用同一 Actor 邮箱串行 + 异步落库路径。
func (s *Server) Forward(ctx context.Context, req *gamerpc.ForwardRequest) (*gamerpc.ForwardResponse, error) {
	if req.Cmd > math.MaxUint16 || req.Act > math.MaxUint16 {
		return nil, fmt.Errorf("protocol command out of range")
	}
	resp, err := s.players.HandleMsg(ctx, req.RoleId, uint16(req.Cmd), uint16(req.Act), req.Body)
	if err != nil {
		return nil, err
	}
	return &gamerpc.ForwardResponse{Body: resp}, nil
}

// runGRPC 在 cfg.GRPCAddr 上启动 gRPC 转发服务（地址为空时跳过）。
func (s *Server) runGRPC(ctx context.Context) error {
	if s.cfg.GRPCAddr == "" {
		<-ctx.Done()
		return nil
	}
	ln, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return err
	}
	options := []grpc.ServerOption{
		grpc.UnaryInterceptor(internalauth.UnaryServerInterceptor(s.cfg.InternalSecret)),
	}
	if s.cfg.InternalTLS.Enabled {
		creds, err := internaltls.ServerCredentials(s.cfg.InternalTLS)
		if err != nil {
			_ = ln.Close()
			return err
		}
		options = append(options, grpc.Creds(creds))
	}
	srv := grpc.NewServer(options...)
	gamerpc.RegisterForwarderServer(srv, s)
	s.grpcSrv = srv
	s.log.Info("game grpc listening", zap.String("addr", s.cfg.GRPCAddr))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	err = srv.Serve(ln)
	if err == grpc.ErrServerStopped {
		return nil
	}
	return err
}

// stopGRPC 优雅停止 gRPC 服务（关停时调用）。
func (s *Server) stopGRPC() {
	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
	}
}
