package app

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ng_http_requests_total",
		Help: "HTTP 请求总数，按 status code 分组",
	}, []string{"code"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ng_http_request_duration_seconds",
		Help:    "HTTP 请求耗时（秒）",
		Buckets: prometheus.DefBuckets,
	}, []string{})
)

// WrapObservability 为 Handler 挂载指标中间件，并在 tracing 启用时包裹 otelhttp。
//
// 参数:
//   - h: 业务 Handler
//   - operation: span 名称，如 "http"
func WrapObservability(h http.Handler, operation string) http.Handler {
	h = WithMetrics(h, globalMetrics)
	if tracingEnabled {
		h = otelhttp.NewHandler(h, operation)
	}
	return h
}

// PrometheusHandler 返回标准 promhttp Handler，挂载于 /metrics/prometheus。
func PrometheusHandler() http.Handler {
	return promhttp.Handler()
}
