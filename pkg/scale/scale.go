// Package scale 定义大规模部署的容量规划常量与 Gate→Game 传输层抽象。
//
// 目标：单区 10 万 CCU 常态，100 万 CCU 峰值（见 docs/architecture-scale.md）。
package scale

import "time"

// 容量规划常量（单区）。
const (
	// TargetCCUBase 设计基线在线人数。
	TargetCCUBase int32 = 100_000
	// TargetCCUPeak 设计峰值在线人数。
	TargetCCUPeak int32 = 1_000_000

	// GateConnPerNode 单 Gate 进程建议最大 TCP 连接数（调优后 1～2 万）。
	GateConnPerNode int32 = 15_000
	// GameCCUPerShard 单 Game 分片建议最大在线（Actor 内存 + 消息吞吐）。
	GameCCUPerShard int32 = 2_000

	// GateHTTPTimeout Gate 调用 Game 的超时（应用 gRPC/长连接池后仍需要）。
	GateHTTPTimeout = 3 * time.Second
	// GateGamePoolSize Gate→Game HTTP 连接池每 host 空闲连接数。
	GateGamePoolSize = 256
)

// GateNodesForCCU 估算所需 Gate 进程数。
func GateNodesForCCU(ccu int32) int32 {
	if ccu <= 0 {
		return 1
	}
	n := (ccu + GateConnPerNode - 1) / GateConnPerNode
	if n < 1 {
		return 1
	}
	return n
}

// GameShardsForCCU 估算所需 Game 分片数。
func GameShardsForCCU(ccu int32) int32 {
	if ccu <= 0 {
		return 1
	}
	n := (ccu + GameCCUPerShard - 1) / GameCCUPerShard
	if n < 1 {
		return 1
	}
	return n
}

// GameForwarder Gate 向 Game 转发玩家消息的统一接口。
// 当前实现：HTTP（pkg/gate 可替换为 gRPC 双向流实现本接口）。
type GameForwarder interface {
	// Forward 将 CmdGame 帧转发到 role 所在分片。
	//
	// 参数:
	//   - roleID: 已鉴权角色 ID
	//   - zoneID: 区服
	//   - cmd, act: 协议字
	//   - body: JSON payload
	//
	// 返回: Game 响应 JSON 字节
	Forward(ctx interface{ Done() <-chan struct{} }, roleID int64, zoneID int32, cmd, act uint16, body []byte) ([]byte, error)
}
