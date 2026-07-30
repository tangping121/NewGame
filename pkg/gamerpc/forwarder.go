// Package gamerpc exposes the generated Gate-to-Game protobuf API behind
// stable names used by the transport layer.
package gamerpc

import (
	"newgame/api/pb"

	"google.golang.org/grpc"
)

const FullMethodForward = pb.GameForwarder_Forward_FullMethodName

type ForwardRequest = pb.GameForwardRequest
type ForwardResponse = pb.GameForwardResponse
type ForwarderServer = pb.GameForwarderServer
type ForwarderClient = pb.GameForwarderClient

func RegisterForwarderServer(s grpc.ServiceRegistrar, impl ForwarderServer) {
	pb.RegisterGameForwarderServer(s, impl)
}

func NewForwarderClient(cc grpc.ClientConnInterface) ForwarderClient {
	return pb.NewGameForwarderClient(cc)
}
