// Package config 负责从 YAML 文件加载各微服务进程的统一配置结构。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Infra 描述 Redis、PostgreSQL、NATS 等公共基础设施的连接地址。
type Infra struct {
	Redis          string   `yaml:"redis"`           // Redis 地址，如 127.0.0.1:6379
	RedisCluster   []string `yaml:"redis_cluster"`   // Redis Cluster 节点列表；非空时优先于 redis
	Postgres       string   `yaml:"postgres"`        // Postgres DSN（单库）
	PostgresShards []string `yaml:"postgres_shards"` // Postgres 分库 DSN 列表；非空时按 role_id 分库
	NATS           string   `yaml:"nats"`            // NATS 地址，如 nats://127.0.0.1:4222
}

// Discovery 基于 Redis 的服务注册与发现相关配置。
type Discovery struct {
	Enabled   bool `yaml:"enabled"`        // 是否启用服务注册；false 时不写入 Redis
	TTLSec    int  `yaml:"ttl_sec"`        // 实例租约 TTL（秒），过期后自动从发现列表移除
	Heartbeat int  `yaml:"heartbeat_sec"`  // 心跳续约间隔（秒），应小于 TTL
}

// TTL 返回实例租约时长；未配置或 <=0 时默认 15 秒。
func (d Discovery) TTL() time.Duration {
	if d.TTLSec <= 0 {
		return 15 * time.Second
	}
	return time.Duration(d.TTLSec) * time.Second
}

// HeartbeatInterval 返回心跳间隔；未配置或 <=0 时默认 5 秒。
func (d Discovery) HeartbeatInterval() time.Duration {
	if d.Heartbeat <= 0 {
		return 5 * time.Second
	}
	return time.Duration(d.Heartbeat) * time.Second
}

// Service 每个微服务进程的基础配置，对应 configs/*.yaml。
type Service struct {
	Name         string        `yaml:"name"`           // 进程显示名，如 game、login
	Type         string        `yaml:"type"`           // 服务类型，用于发现索引，如 game、gate
	HTTPAddr     string        `yaml:"http_addr"`      // HTTP 监听地址，如 :9100
	GRPCAddr     string        `yaml:"grpc_addr"`      // gRPC 监听地址（预留）
	TCPAddr      string        `yaml:"tcp_addr"`       // TCP 监听地址，Gate 使用，如 :9000
	ZoneID       int32         `yaml:"zone_id"`        // 所属区服 ID，1=一区，2=二区
	ZoneMode     string        `yaml:"zone_mode"`      // Gate 区服模式：dedicated=校验区服；hub=跨区路由
	MaxConnPerIP int           `yaml:"max_conn_per_ip"` // Gate 单 IP 最大并发 TCP 连接数
	Infra        Infra         `yaml:"infra"`          // 基础设施连接
	Discovery    Discovery     `yaml:"discovery"`      // 服务发现配置
	Observability Observability `yaml:"observability"` // 可观测性配置
	CrossZoneMatch bool           `yaml:"cross_zone_match"` // Match 专用：true 时使用 Redis 跨服匹配队列
	LogLevel       string         `yaml:"log_level"`        // 日志级别：debug/info/warn/error
	Scale          Scale          `yaml:"scale"`            // 大规模分片与容量配置（见 docs/architecture-scale.md）
	Gate           GateScale      `yaml:"gate"`             // Gate 转发层调优
	InternalSecret string         `yaml:"internal_secret"`  // 内部接口鉴权密钥；空则不校验（开发）
	Rank           RankConfig     `yaml:"rank"`             // 排行榜调优
}

// RankConfig 排行榜容量与裁剪配置。
type RankConfig struct {
	Cap            int `yaml:"cap"`              // 每个榜保留 TopN，<=0 时默认 10000
	TrimIntervalSec int `yaml:"trim_interval_sec"` // 后台裁剪间隔秒，<=0 时默认 60
}

// RankCap 返回榜单保留上限。
func (r RankConfig) RankCap() int {
	if r.Cap <= 0 {
		return 10000
	}
	return r.Cap
}

// TrimInterval 返回裁剪间隔。
func (r RankConfig) TrimInterval() time.Duration {
	if r.TrimIntervalSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(r.TrimIntervalSec) * time.Second
}

// Scale Game/Gate 分片与容量规划配置。
type Scale struct {
	ShardID         int32  `yaml:"shard_id"`           // 本 Game 进程分片编号，0..shard_count-1
	ShardCount      int32  `yaml:"shard_count"`        // 单区 Game 分片总数，10 万 CCU 建议 50，100 万建议 500
	MaxCCU          int32  `yaml:"max_ccu"`            // 本片最大在线告警阈值，默认 2000
	SaveMode        string `yaml:"save_mode"`          // 落库模式：async | sync；默认 sync
	SaveIntervalSec int    `yaml:"save_interval_sec"`  // async 模式 flush 间隔（秒），默认 10
	SaveConcurrency int    `yaml:"save_concurrency"`   // async flush 并发数，默认 16
}

// SaveInterval 返回异步落库间隔。
func (s Scale) SaveInterval() time.Duration {
	if s.SaveIntervalSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(s.SaveIntervalSec) * time.Second
}

// GateScale Gate 连接 Game 的传输层与连接处理配置。
type GateScale struct {
	GameTransport    string `yaml:"game_transport"`     // http_pool | grpc
	GamePoolSize     int    `yaml:"game_pool_size"`     // HTTP 连接池大小，默认 256
	ReadTimeoutSec   int    `yaml:"read_timeout_sec"`   // 客户端读超时（心跳窗口），默认 60
	WriteTimeoutSec  int    `yaml:"write_timeout_sec"`  // 客户端写超时，默认 5
	ForwardTimeoutMs int    `yaml:"forward_timeout_ms"` // 转发 Game 超时，默认 3000
	Acceptors        int    `yaml:"acceptors"`          // TCP Accept 协程数，默认 1
}

// ReadTimeout 客户端读超时（含心跳间隔余量）。
func (g GateScale) ReadTimeout() time.Duration {
	if g.ReadTimeoutSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(g.ReadTimeoutSec) * time.Second
}

// WriteTimeout 客户端写超时。
func (g GateScale) WriteTimeout() time.Duration {
	if g.WriteTimeoutSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(g.WriteTimeoutSec) * time.Second
}

// ForwardTimeout 转发到 Game 的超时。
func (g GateScale) ForwardTimeout() time.Duration {
	if g.ForwardTimeoutMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(g.ForwardTimeoutMs) * time.Millisecond
}

// AcceptorCount Accept 协程数。
func (g GateScale) AcceptorCount() int {
	if g.Acceptors <= 0 {
		return 1
	}
	return g.Acceptors
}

// Observability 可观测性配置，包括链路追踪与 Prometheus 指标。
type Observability struct {
	Tracing    TracingConfig `yaml:"tracing"`    // OpenTelemetry 追踪配置
	Prometheus bool          `yaml:"prometheus"` // 是否暴露 /metrics/prometheus（当前始终注册）
}

// TracingConfig OpenTelemetry 分布式追踪配置。
type TracingConfig struct {
	Enabled      bool   `yaml:"enabled"`       // 是否启用追踪
	OTLPEndpoint string `yaml:"otlp_endpoint"` // OTLP HTTP 采集端点；空则输出到 stdout
}

// Load 从 path 指定的 YAML 文件读取并反序列化到 out。
//
// 参数:
//   - path: 配置文件路径，如 configs/game.yaml
//   - out: 目标结构体指针，通常为 *config.Service
//
// 返回: 文件不存在、读取失败或 YAML 解析失败时返回错误
func Load(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	if svc, ok := out.(*Service); ok {
		applyEnvOverrides(svc)
		if err := svc.Validate(); err != nil {
			return fmt.Errorf("invalid config %s: %w", path, err)
		}
	}
	return nil
}

// Validate 校验配置自洽性，启动时 fail-fast，避免错误配置上线后才暴露。
func (s *Service) Validate() error {
	if s.Scale.ShardCount < 0 {
		return fmt.Errorf("scale.shard_count must be >= 0, got %d", s.Scale.ShardCount)
	}
	if s.Scale.ShardCount > 0 {
		if s.Scale.ShardID < 0 || s.Scale.ShardID >= s.Scale.ShardCount {
			return fmt.Errorf("scale.shard_id %d out of range [0,%d)", s.Scale.ShardID, s.Scale.ShardCount)
		}
	}
	switch s.Scale.SaveMode {
	case "", "sync", "async":
	default:
		return fmt.Errorf("scale.save_mode must be sync|async, got %q", s.Scale.SaveMode)
	}
	switch s.Gate.GameTransport {
	case "", "http_pool", "grpc":
	default:
		return fmt.Errorf("gate.game_transport must be http_pool|grpc, got %q", s.Gate.GameTransport)
	}
	return nil
}

// applyEnvOverrides 用环境变量覆盖配置，便于容器/K8s 部署（StatefulSet 按 pod 注入分片号）。
//
// 支持的变量：
//   - NG_NAME / NG_ZONE_ID
//   - NG_HTTP_ADDR / NG_TCP_ADDR / NG_GRPC_ADDR
//   - NG_SHARD_ID / NG_SHARD_COUNT；或 POD_NAME（取末尾序号作为 shard_id）
//   - NG_REDIS / NG_REDIS_CLUSTER（逗号分隔）/ NG_NATS
//   - NG_POSTGRES / NG_POSTGRES_SHARDS（逗号分隔）
func applyEnvOverrides(svc *Service) {
	if v := os.Getenv("NG_NAME"); v != "" {
		svc.Name = v
	}
	if v := os.Getenv("NG_ZONE_ID"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			svc.ZoneID = int32(n)
		}
	}
	if v := os.Getenv("NG_HTTP_ADDR"); v != "" {
		svc.HTTPAddr = v
	}
	if v := os.Getenv("NG_TCP_ADDR"); v != "" {
		svc.TCPAddr = v
	}
	if v := os.Getenv("NG_GRPC_ADDR"); v != "" {
		svc.GRPCAddr = v
	}
	if v := os.Getenv("NG_SHARD_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			svc.Scale.ShardCount = int32(n)
		}
	}
	if id, ok := shardIDFromEnv(); ok {
		svc.Scale.ShardID = id
	}
	if v := os.Getenv("NG_REDIS"); v != "" {
		svc.Infra.Redis = v
	}
	if v := os.Getenv("NG_REDIS_CLUSTER"); v != "" {
		svc.Infra.RedisCluster = splitCSV(v)
	}
	if v := os.Getenv("NG_NATS"); v != "" {
		svc.Infra.NATS = v
	}
	if v := os.Getenv("NG_INTERNAL_SECRET"); v != "" {
		svc.InternalSecret = v
	}
	if v := os.Getenv("NG_POSTGRES"); v != "" {
		svc.Infra.Postgres = v
	}
	if v := os.Getenv("NG_POSTGRES_SHARDS"); v != "" {
		svc.Infra.PostgresShards = splitCSV(v)
	}
}

// shardIDFromEnv 解析分片号：优先 NG_SHARD_ID，其次 POD_NAME 末尾序号（StatefulSet）。
func shardIDFromEnv() (int32, bool) {
	if v := os.Getenv("NG_SHARD_ID"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return int32(n), true
		}
	}
	// StatefulSet pod 名形如 game-shard-3，取末尾数字。
	if pod := os.Getenv("POD_NAME"); pod != "" {
		if idx := strings.LastIndex(pod, "-"); idx >= 0 && idx+1 < len(pod) {
			if n, err := strconv.Atoi(pod[idx+1:]); err == nil {
				return int32(n), true
			}
		}
	}
	return 0, false
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
