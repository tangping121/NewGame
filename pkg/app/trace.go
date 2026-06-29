package app

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.uber.org/zap"

	"newgame/pkg/config"
)

var tracingEnabled bool

// InitTracing 初始化 OpenTelemetry TracerProvider。
//
// 参数:
//   - cfg: observability.tracing 配置块
//   - serviceName: 服务名，写入 span resource（通常为 config.Service.Name）
//   - log: 日志器
//
// 返回:
//   - shutdown: 进程退出时调用的关闭函数；未启用 tracing 时为 nil
//   - error: exporter 或 resource 创建失败
func InitTracing(cfg config.Observability, serviceName string, log *zap.Logger) (func(context.Context) error, error) {
	if !cfg.Tracing.Enabled {
		return nil, nil
	}
	var exp sdktrace.SpanExporter
	var err error
	if cfg.Tracing.OTLPEndpoint != "" {
		exp, err = otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpointURL(cfg.Tracing.OTLPEndpoint),
			otlptracehttp.WithInsecure(),
		)
	} else {
		exp, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	if err != nil {
		return nil, fmt.Errorf("trace exporter: %w", err)
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	tracingEnabled = true
	log.Info("tracing enabled", zap.String("service", serviceName))
	return tp.Shutdown, nil
}
