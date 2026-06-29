package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	loginURL := flag.String("login", "http://127.0.0.1:8080/api/login", "login endpoint")
	zone := flag.Int("zone", 1, "zone id")
	workers := flag.Int("workers", 10, "concurrent workers")
	requests := flag.Int("n", 100, "total login requests")
	flag.Parse()

	fmt.Printf("loadtest: workers=%d n=%d zone=%d\n", *workers, *requests, *zone)

	var ok, fail atomic.Uint64
	start := time.Now()
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup

	for i := 0; i < *requests; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			user := fmt.Sprintf("load_%d_%d", time.Now().UnixNano(), idx)
			body, _ := json.Marshal(map[string]any{
				"username": user,
				"password": user,
				"zone_id":  *zone,
			})
			resp, err := http.Post(*loginURL, "application/json", bytes.NewReader(body))
			if err != nil {
				fail.Add(1)
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			var out struct {
				Code int `json:"code"`
			}
			_ = json.Unmarshal(b, &out)
			if resp.StatusCode == 200 && out.Code == 0 {
				ok.Add(1)
			} else {
				fail.Add(1)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	total := ok.Load() + fail.Load()
	qps := float64(total) / elapsed.Seconds()
	fmt.Printf("done in %s: ok=%d fail=%d qps=%.1f\n", elapsed.Round(time.Millisecond), ok.Load(), fail.Load(), qps)

	healthURL := "http://127.0.0.1:8080/metrics"
	if resp, err := http.Get(healthURL); err == nil {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("login metrics: %s\n", string(b))
	}
}
