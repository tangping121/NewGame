// Command cctest 是 Gate TCP 长连接压测工具：建立海量并发长连接，
// 持续发送 CmdGame/CmdPing，统计连接成功率、吞吐与延迟分位（P50/P90/P99）。
//
// 用法示例：
//
//	go run ./tools/cctest -conns 5000 -duration 30s -rate 1
//
// 验证目标（见 docs/architecture-scale.md）：单 Gate ~1.5 万长连接，
// 单分片 ~2000 CCU，转发 P99 < 100ms。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"newgame/api/pb"
	"newgame/pkg/protocol"
)

func main() {
	loginURL := flag.String("login", "http://127.0.0.1:8080/api/login", "登录 HTTP 接口")
	gateOverride := flag.String("gate", "", "Gate TCP 地址；空则用登录响应的 gate_addr")
	zone := flag.Int("zone", 1, "区服 ID")
	conns := flag.Int("conns", 1000, "并发长连接数")
	duration := flag.Duration("duration", 30*time.Second, "压测时长")
	rate := flag.Float64("rate", 1, "每连接每秒 CmdGame 次数")
	pingInterval := flag.Duration("ping", 30*time.Second, "心跳间隔")
	dialConc := flag.Int("dial-concurrency", 500, "建连阶段并发度")
	prefix := flag.String("userprefix", "cc", "压测账号前缀")
	flag.Parse()

	fmt.Printf("cctest: conns=%d duration=%s rate=%.2f/s zone=%d\n", *conns, *duration, *rate, *zone)

	var st stats
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	dialSem := make(chan struct{}, *dialConc)
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dialSem <- struct{}{}
			w := &worker{
				loginURL:     *loginURL,
				gateOverride: *gateOverride,
				zone:         int32(*zone),
				user:         fmt.Sprintf("%s_%d", *prefix, idx),
				rate:         *rate,
				pingInterval: *pingInterval,
				st:           &st,
			}
			conn, ok := w.connect()
			<-dialSem
			if !ok {
				return
			}
			defer conn.Close()
			st.connected.Add(1)
			w.loop(ctx, conn)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	st.report(elapsed)
}

type worker struct {
	loginURL     string
	gateOverride string
	zone         int32
	user         string
	rate         float64
	pingInterval time.Duration
	st           *stats
}

// connect 登录并建立 TCP 连接，完成 Gate CmdLogin 鉴权。
func (w *worker) connect() (net.Conn, bool) {
	login, err := httpLogin(w.loginURL, w.user, w.zone)
	if err != nil || login.Code != 0 {
		w.st.loginFail.Add(1)
		return nil, false
	}
	gateAddr := w.gateOverride
	if gateAddr == "" {
		gateAddr = login.GateAddr
	}
	if gateAddr == "" {
		gateAddr = "127.0.0.1:9000"
	}
	conn, err := net.DialTimeout("tcp", gateAddr, 5*time.Second)
	if err != nil {
		w.st.dialFail.Add(1)
		return nil, false
	}
	body, _ := json.Marshal(pb.EnterGateRequest{Token: login.Token})
	if _, err := w.roundtrip(conn, protocol.Frame{Cmd: protocol.CmdLogin, Act: protocol.ActLogin, Body: body}); err != nil {
		w.st.authFail.Add(1)
		_ = conn.Close()
		return nil, false
	}
	return conn, true
}

// loop 持续按 rate 发送 CmdGame，并按 pingInterval 发送心跳，直至 ctx 结束。
func (w *worker) loop(ctx context.Context, conn net.Conn) {
	interval := time.Second
	if w.rate > 0 {
		interval = time.Duration(float64(time.Second) / w.rate)
	}
	msgTicker := time.NewTicker(interval)
	defer msgTicker.Stop()
	pingTicker := time.NewTicker(w.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			if _, err := w.roundtrip(conn, protocol.Frame{Cmd: protocol.CmdPing, Act: protocol.ActPing, Body: []byte(`{}`)}); err != nil {
				w.st.errors.Add(1)
				return
			}
		case <-msgTicker.C:
			t0 := time.Now()
			_, err := w.roundtrip(conn, protocol.Frame{Cmd: protocol.CmdGame, Act: protocol.ActPlayerData, Body: []byte(`{}`)})
			if err != nil {
				w.st.errors.Add(1)
				return
			}
			w.st.recordLatency(time.Since(t0))
			w.st.framesOK.Add(1)
		}
	}
}

// roundtrip 写一帧并读回一帧（同步请求-响应）。
func (w *worker) roundtrip(conn net.Conn, f protocol.Frame) ([]byte, error) {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(protocol.Encode(f)); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	size := int(hdr[0])<<8 | int(hdr[1])
	if size < protocol.HeaderSize {
		return nil, fmt.Errorf("short frame")
	}
	buf := make([]byte, size-2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	dec, err := protocol.Decode(buf)
	if err != nil {
		return nil, err
	}
	return dec.Body, nil
}

func httpLogin(url, user string, zone int32) (*pb.LoginResponse, error) {
	body, _ := json.Marshal(&pb.LoginRequest{Username: user, Password: user, ZoneId: zone})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out := new(pb.LoginResponse)
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return nil, err
	}
	return out, nil
}
