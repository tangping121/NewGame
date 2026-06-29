package redis

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ErrorsTotal Redis 操作失败计数，按 service（调用方）与 op（操作名）分组。
//
// 典型 op：presence_store、presence_renew、rank_zadd、guildwar_zincr 等。
// 在 Grafana 可对 ng_redis_errors_total 配置告警规则。
var ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ng_redis_errors_total",
	Help: "Redis 操作失败次数（按 service/op 分组）",
}, []string{"service", "op"})

// RecordError 记录一次 Redis 失败；service 为调用方服务名，op 为操作简称。
func RecordError(service, op string) {
	ErrorsTotal.WithLabelValues(service, op).Inc()
}
