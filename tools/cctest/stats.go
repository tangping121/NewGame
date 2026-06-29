package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// latencyBuckets 毫秒级延迟直方图边界（最后一桶为溢出 >=1000ms）。
const latencyBuckets = 1001 // 0..1000ms

// stats 压测计数与延迟直方图，全部并发安全。
type stats struct {
	connected atomic.Int64 // 成功建连并鉴权
	loginFail atomic.Int64 // HTTP 登录失败
	dialFail  atomic.Int64 // TCP 建连失败
	authFail  atomic.Int64 // Gate 鉴权失败
	framesOK  atomic.Int64 // 成功收发的 CmdGame 帧
	errors    atomic.Int64 // 运行期读写错误（连接断开）

	hist  [latencyBuckets]atomic.Int64 // 毫秒桶
	total atomic.Int64                 // 延迟样本数
	sumUS atomic.Int64                 // 延迟总微秒，用于均值
}

// recordLatency 记录一次 RTT。
func (s *stats) recordLatency(d time.Duration) {
	ms := int(d.Milliseconds())
	if ms < 0 {
		ms = 0
	}
	if ms >= latencyBuckets {
		ms = latencyBuckets - 1
	}
	s.hist[ms].Add(1)
	s.total.Add(1)
	s.sumUS.Add(d.Microseconds())
}

// percentile 返回给定分位（0~1）的毫秒延迟。
func (s *stats) percentile(p float64) int {
	total := s.total.Load()
	if total == 0 {
		return 0
	}
	target := int64(p * float64(total))
	var cum int64
	for ms := 0; ms < latencyBuckets; ms++ {
		cum += s.hist[ms].Load()
		if cum >= target {
			return ms
		}
	}
	return latencyBuckets - 1
}

// report 打印压测结果。
func (s *stats) report(elapsed time.Duration) {
	total := s.total.Load()
	var avgMS float64
	if total > 0 {
		avgMS = float64(s.sumUS.Load()) / float64(total) / 1000.0
	}
	framesOK := s.framesOK.Load()
	qps := float64(framesOK) / elapsed.Seconds()

	fmt.Println("---- cctest result ----")
	fmt.Printf("elapsed:        %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("connected:      %d\n", s.connected.Load())
	fmt.Printf("login_fail:     %d\n", s.loginFail.Load())
	fmt.Printf("dial_fail:      %d\n", s.dialFail.Load())
	fmt.Printf("auth_fail:      %d\n", s.authFail.Load())
	fmt.Printf("frames_ok:      %d\n", framesOK)
	fmt.Printf("errors:         %d\n", s.errors.Load())
	fmt.Printf("throughput:     %.1f frames/s\n", qps)
	fmt.Printf("latency avg:    %.2f ms\n", avgMS)
	fmt.Printf("latency p50:    %d ms\n", s.percentile(0.50))
	fmt.Printf("latency p90:    %d ms\n", s.percentile(0.90))
	fmt.Printf("latency p99:    %d ms\n", s.percentile(0.99))
}
