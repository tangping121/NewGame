package internal

import (
	"context"
	"net"

	"newgame/pkg/gamerpc"
	"newgame/pkg/internalauth"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Forward 实现 gamerpc.ForwarderServer：执行 Gate 经 gRPC 转发的玩家消息。
//
// 与 HTTP /internal/player/msg 等价，复用同一 Actor 邮箱串行 + 异步落库路径。
func (s *Server) Forward(ctx context.Context, req *gamerpc.ForwardRequest) (*gamerpc.ForwardResponse, error) {
	resp, err := s.players.HandleMsg(ctx, req.RoleID, req.Cmd, req.Act, req.Body)
	if err != nil {
		return nil, err
	}
	return &gamerpc.ForwardResponse{Body: resp}, nil
}

// runGRPC 在 cfg.GRPCAddr 上启动 gRPC 转发服务（地址为空时跳过）。
func (s *Server) runGRPC() {
	if s.cfg.GRPCAddr == "" {
		return
	}
	ln, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		s.log.Error("game grpc listen failed", zap.String("addr", s.cfg.GRPCAddr), zap.Error(err))
		return
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(internalauth.UnaryServerInterceptor(s.cfg.InternalSecret)))
	gamerpc.RegisterForwarderServer(srv, s)
	s.log.Info("game grpc listening", zap.String("addr", s.cfg.GRPCAddr))
	if err := srv.Serve(ln); err != nil {
		s.log.Error("game grpc serve failed", zap.Error(err))
	}
}
