package app

import (
	"context"
	"time"

	"newgame/pkg/config"
	"newgame/pkg/discovery"
	redisx "newgame/pkg/redis"

	"go.uber.org/zap"
)

// StartDiscovery 若配置启用，则将本进程注册到 Redis 服务发现。
//
// 参数:
//   - cfg: 完整服务配置
//   - log: 日志器
//
// 返回:
//   - *Lifecycle: 生命周期句柄，退出时 Stop；未启用时为 nil
//   - *Registry: 注册表，可供本进程 Pick 其他服务；未启用时为 nil
func StartDiscovery(cfg config.Service, log *zap.Logger) (*discovery.Lifecycle, *discovery.Registry, error) {
	if !cfg.Discovery.Enabled || cfg.Infra.Redis == "" {
		return nil, nil, nil
	}
	rdb := redisx.New(cfg.Infra.Redis, cfg.Infra.RedisCluster)
	reg := discovery.NewRegistry(rdb, cfg.Discovery.TTL())
	inst := discovery.InstanceFromConfig(cfg)
	lc, err := reg.Start(inst, cfg.Discovery.HeartbeatInterval(), log)
	if err != nil {
		return nil, nil, err
	}
	return lc, reg, nil
}

// RunWithDiscovery 标准进程主循环：可选 tracing + 注册发现 + 执行 fn + 清理。
//
// 参数:
//   - cfg: 服务配置
//   - log: 日志器
//   - fn: 实际阻塞逻辑，通常为 app.RunHTTP
func RunWithDiscovery(cfg config.Service, log *zap.Logger, fn func() error) error {
	shutdownTrace, err := InitTracing(cfg.Observability, cfg.Name, log)
	if err != nil {
		log.Warn("tracing init failed", zap.Error(err))
	}
	if shutdownTrace != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTrace(ctx)
		}()
	}
	lc, _, err := StartDiscovery(cfg, log)
	if err != nil {
		return err
	}
	if lc != nil {
		defer lc.Stop()
	}
	return fn()
}
