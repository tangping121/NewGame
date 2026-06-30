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
// 返回: 填充 ID、Name、ZoneID 及对外广播地址（advertise_* 优先于 PublishAddr）
func InstanceFromConfig(cfg config.Service) Instance {
	httpAddr := AdvertiseAddr(cfg.HTTPAddr, cfg.AdvertiseHTTPAddr)
	tcpAddr := ""
	if cfg.TCPAddr != "" {
		tcpAddr = AdvertiseAddr(cfg.TCPAddr, cfg.AdvertiseTCPAddr)
	}
	grpcAddr := ""
	if cfg.GRPCAddr != "" {
		grpcAddr = AdvertiseAddr(cfg.GRPCAddr, cfg.AdvertiseGRPCAddr)
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
	addr := AdvertiseAddr(cfg.HTTPAddr, cfg.AdvertiseHTTPAddr)
	if addr == "" {
		addr = AdvertiseAddr(cfg.TCPAddr, cfg.AdvertiseTCPAddr)
	}
	return fmt.Sprintf("%s-z%d-%s-%d", cfg.Type, cfg.ZoneID, addr, os.Getpid())
}

// AdvertiseAddr 返回对外注册/客户端可见地址。
//
// 参数:
//   - listen: 进程监听地址，如 :9000、0.0.0.0:9000
//   - advertise: 显式对外地址，如 gate.game.com:9000；非空时直接返回
//
// 生产环境 Gate 应配置 advertise_tcp_addr 为 LB/域名，避免把 Pod 内网 IP 写入 gate_addr。
func AdvertiseAddr(listen, advertise string) string {
	if advertise != "" {
		return advertise
	}
	return PublishAddr(listen)
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
