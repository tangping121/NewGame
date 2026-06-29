package app

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// Metrics 进程内 HTTP 基础指标计数器（与 Prometheus 双写）。
type Metrics struct {
	Requests  atomic.Uint64 // 总请求数
	Errors5xx atomic.Uint64 // HTTP 5xx 响应次数
	LatencyNs atomic.Uint64 // 累计延迟（纳秒），用于算平均值
}

var globalMetrics = &Metrics{}

// GlobalMetrics 返回进程全局 Metrics 实例。
func GlobalMetrics() *Metrics { return globalMetrics }

// Snapshot 导出 JSON /metrics 用的快照。
func (m *Metrics) Snapshot() map[string]any {
	req := m.Requests.Load()
	if req == 0 {
		return map[string]any{"requests": 0, "errors_5xx": 0, "avg_latency_ms": 0}
	}
	return map[string]any{
		"requests":       req,
		"errors_5xx":     m.Errors5xx.Load(),
		"avg_latency_ms": float64(m.LatencyNs.Load()) / float64(req) / 1e6,
	}
}

// Handler 作为 GET /metrics 的 JSON 处理器。
func (m *Metrics) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.Snapshot())
}

type metricsResponseWriter struct {
	http.ResponseWriter
	metrics *Metrics
	start   time.Time
	status  int // 实际 HTTP 状态码，WriteHeader 前默认为 200
}

func (w *metricsResponseWriter) WriteHeader(code int) {
	w.status = code
	if code >= 500 {
		w.metrics.Errors5xx.Add(1)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsResponseWriter) statusCode() int {
	if w.status == 0 {
		return 200
	}
	return w.status
}

// WithMetrics 包装 Handler，统计 QPS、状态码分布与耗时，并写入 Prometheus。
//
// 参数:
//   - h: 下游 Handler
//   - m: 指标实例；nil 时使用 globalMetrics
func WithMetrics(h http.Handler, m *Metrics) http.Handler {
	if m == nil {
		m = globalMetrics
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m.Requests.Add(1)
		mw := &metricsResponseWriter{ResponseWriter: w, metrics: m, start: start}
		h.ServeHTTP(mw, r)
		elapsed := time.Since(start)
		m.LatencyNs.Add(uint64(elapsed.Nanoseconds()))
		code := strconv.Itoa(mw.statusCode())
		httpRequestsTotal.WithLabelValues(code).Inc()
		httpRequestDuration.WithLabelValues().Observe(elapsed.Seconds())
	})
}
