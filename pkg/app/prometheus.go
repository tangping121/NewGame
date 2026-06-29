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

	// GateConnections 当前 Gate 在线 TCP 连接数（告警阈值约 12000/节点）。
	GateConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ng_gate_connections",
		Help: "Gate 当前在线 TCP 连接数",
	})

	// GateForwardLatency Gate→Game 单帧转发耗时（秒）。
	GateForwardLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ng_gate_forward_latency_seconds",
		Help:    "Gate 转发到 Game 的单帧耗时（秒）",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})

	// GameOnline 当前 Game 分片在线玩家 Actor 数（告警阈值约 1800/片）。
	GameOnline = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ng_online_per_shard",
		Help: "本 Game 分片在线玩家数",
	})

	// GameSaveQueueDepth 异步落库待写队列深度（告警阈值约 10000）。
	GameSaveQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ng_game_save_queue_depth",
		Help: "异步落库待写玩家数",
	})

	// GameSaveTotal 累计成功落库次数。
	GameSaveTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_game_save_total",
		Help: "累计成功落库次数",
	})

	// GameSaveFailed 累计落库失败次数（已重入队重试）。
	GameSaveFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_game_save_failed_total",
		Help: "累计落库失败次数",
	})
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
