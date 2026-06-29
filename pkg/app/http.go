// Package app 提供 HTTP 服务启动、健康检查、指标与可观测性中间件。
package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// RunHTTP 启动 HTTP 服务并阻塞至收到 SIGINT/SIGTERM。
//
// 参数:
//   - logger: zap 日志器
//   - addr: 监听地址，如 :9100
//   - handler: 业务路由 Handler，会自动包裹指标与追踪中间件
//
// 返回: Shutdown 超时或错误
func RunHTTP(logger *zap.Logger, addr string, handler http.Handler) error {
	handler = WrapObservability(handler, "http")
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("http listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("http serve", zap.Error(err))
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// HealthHandler 标准健康检查，返回 200 与 body "ok"。
func HealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// MountHealth 向 mux 注册 /health、/metrics（JSON）、/metrics/prometheus。
func MountHealth(mux *http.ServeMux) {
	mux.HandleFunc("/health", HealthHandler)
	mux.HandleFunc("/metrics", globalMetrics.Handler)
	mux.Handle("/metrics/prometheus", PrometheusHandler())
}

// MustConfigFlag 从 os.Args 解析 -config 参数。
//
// 返回: 配置文件路径，默认 ./configs/service.yaml；缺失时 panic
func MustConfigFlag() string {
	cfg := "./configs/service.yaml"
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-config" && i+1 < len(os.Args) {
			cfg = os.Args[i+1]
		}
	}
	if cfg == "" {
		panic(fmt.Errorf("missing -config"))
	}
	return cfg
}
