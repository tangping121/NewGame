package discovery

import (
	"fmt"
	"net"
	"os"

	"newgame/pkg/config"
	"newgame/pkg/shard"
)

// Instance 注册到 Redis 的服务实例，包含可被其他服务调用的地址信息。
type Instance struct {
	ID       string `json:"id"`        // 实例唯一 ID，含类型、区服、地址、进程号
	Name     string `json:"name"`      // 服务类型名，与 config.Service.Type 一致，如 game、gate
	ZoneID   int32  `json:"zone_id"`   // 所属区服 ID
	HTTPAddr string `json:"http_addr"` // 对外可达的 HTTP host:port
	TCPAddr  string `json:"tcp_addr"`  // 对外可达的 TCP host:port，Gate 使用
	GRPCAddr string `json:"grpc_addr"` // 对外可达的 gRPC host:port，Gate→Game 转发使用
}

// InstanceFromConfig 根据当前进程配置生成待注册实例。
//
// 参数:
//   - cfg: 本进程加载的 config.Service
//
// 返回: 填充 ID、Name、ZoneID 及 PublishAddr 处理后的 HTTP/TCP 地址
func InstanceFromConfig(cfg config.Service) Instance {
	httpAddr := PublishAddr(cfg.HTTPAddr)
	tcpAddr := ""
	if cfg.TCPAddr != "" {
		tcpAddr = PublishAddr(cfg.TCPAddr)
	}
	grpcAddr := ""
	if cfg.GRPCAddr != "" {
		grpcAddr = PublishAddr(cfg.GRPCAddr)
	}
	name := cfg.Type
	if cfg.Scale.ShardCount > 0 && cfg.Type == "game" {
		name = shard.ServiceName(cfg.Scale.ShardID)
	}
	return Instance{
		ID:       instanceID(cfg),
		Name:     name,
		ZoneID:   cfg.ZoneID,
		HTTPAddr: httpAddr,
		TCPAddr:  tcpAddr,
		GRPCAddr: grpcAddr,
	}
}

func instanceID(cfg config.Service) string {
	addr := PublishAddr(cfg.HTTPAddr)
	if addr == "" {
		addr = PublishAddr(cfg.TCPAddr)
	}
	return fmt.Sprintf("%s-z%d-%s-%d", cfg.Type, cfg.ZoneID, addr, os.Getpid())
}

// PublishAddr 将监听地址转为客户端/其他服务可连接的 host:port。
//
// 参数:
//   - listenAddr: 如 ":9100"、"0.0.0.0:9000"
//
// 返回: 如 "127.0.0.1:9100"；空串原样返回
func PublishAddr(listenAddr string) string {
	if listenAddr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return listenAddr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// HTTPBase 返回带 http:// 前缀的基址，用于拼接 API 路径。
//
// 返回: 如 "http://127.0.0.1:9100"；HTTPAddr 为空时返回 ""
func (i Instance) HTTPBase() string {
	if i.HTTPAddr == "" {
		return ""
	}
	return "http://" + i.HTTPAddr
}
