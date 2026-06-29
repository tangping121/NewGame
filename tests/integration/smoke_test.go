//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"newgame/api/pb"
	"newgame/pkg/protocol"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func requireService(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Skipf("service not running: %s (%v)", url, err)
	}
	_ = resp.Body.Close()
}

func TestIntegrationLoginAndGate(t *testing.T) {
	loginURL := env("LOGIN_URL", "http://127.0.0.1:8080")
	requireService(t, loginURL+"/health")

	body, _ := json.Marshal(pb.LoginRequest{Username: "int_user", Password: "int_pass", ZoneId: 1})
	resp, err := http.Post(loginURL+"/api/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var login pb.LoginResponse
	_ = json.NewDecoder(resp.Body).Decode(&login)
	if login.Code != 0 {
		t.Fatalf("login failed: %+v", login)
	}

	conn, err := net.DialTimeout("tcp", login.GateAddr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tokenBody, _ := json.Marshal(pb.EnterGateRequest{Token: login.Token})
	frame := protocol.Frame{Cmd: protocol.CmdLogin, Act: protocol.ActLogin, Body: tokenBody}
	_, _ = conn.Write(protocol.Encode(frame))
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationGlobalRank(t *testing.T) {
	rankURL := env("RANK_URL", "http://127.0.0.1:9600")
	requireService(t, rankURL+"/health")
	resp, err := http.Get(rankURL + "/api/rank/global/top?board=dungeon")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestIntegrationZones(t *testing.T) {
	loginURL := env("LOGIN_URL", "http://127.0.0.1:8080")
	requireService(t, loginURL+"/health")
	resp, err := http.Get(loginURL + "/api/zones")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Code  int `json:"code"`
		Zones []any `json:"zones"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Code != 0 {
		t.Fatalf("zones api failed")
	}
}
