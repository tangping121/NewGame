package gamerpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"newgame/pkg/gamerpc"
	"newgame/pkg/rpccodec"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// echoServer 回显请求，验证 gRPC + JSON codec 往返。
type echoServer struct{}

func (echoServer) Forward(_ context.Context, req *gamerpc.ForwardRequest) (*gamerpc.ForwardResponse, error) {
	return &gamerpc.ForwardResponse{Body: req.Body}, nil
}

func TestForwarderRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	gamerpc.RegisterForwarderServer(srv, echoServer{})
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	cc, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpccodec.JSON{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := gamerpc.NewForwarderClient(cc).Forward(ctx, &gamerpc.ForwardRequest{
		RoleID: 10001, ZoneID: 1, Cmd: 2, Act: 2, Body: []byte(`{"hello":"world"}`),
	})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if string(resp.Body) != `{"hello":"world"}` {
		t.Fatalf("unexpected body %s", resp.Body)
	}
}
