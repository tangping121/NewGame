package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"newgame/api/pb"
	"newgame/pkg/protocol"
)

func main() {
	loginURL := env("LOGIN_URL", "http://127.0.0.1:8080/api/login")
	matchURL := env("MATCH_URL", "http://127.0.0.1:9200/api/match/join")
	gameURL := env("GAME_URL", "http://127.0.0.1:9100")
	rankURL := env("RANK_URL", "http://127.0.0.1:9600/api/rank/top?board=dungeon")
	battleSettle := env("BATTLE_SETTLE_URL", "http://127.0.0.1:9300/api/battle/settle")

	fmt.Println("== robot smoke test ==")

	loginA := mustLogin(loginURL, "robot_a", "robot_a", 1)
	fmt.Printf("login A ok role=%d gate=%s\n", loginA.RoleId, loginA.GateAddr)

	loginB := mustLogin(loginURL, "robot_b", "robot_b", 1)
	fmt.Printf("login B ok role=%d\n", loginB.RoleId)

	gateTCP(loginA.GateAddr, loginA.Token)

	gameFrame(loginA.GateAddr, loginA.Token)

	mustDungeonPass(gameURL, loginA.RoleId)

	time.Sleep(500 * time.Millisecond)
	mustRank(rankURL, loginA.RoleId)

	roomID := mustMatch(matchURL, loginA.RoleId, 1)
	if roomID == "" {
		roomID = mustMatch(matchURL, loginB.RoleId, 1)
	}
	if roomID == "" {
		fail("match", fmt.Errorf("no room_id after two joins"))
	}
	fmt.Printf("match ok room=%s\n", roomID)

	mustBattleSettle(battleSettle, roomID, loginA.RoleId, true, 100)
	mustBattleSettle(battleSettle, roomID, loginB.RoleId, false, 10)

	fmt.Println("smoke test passed")
}

func mustLogin(url, user, pass string, zone int32) *pb.LoginResponse {
	body, _ := json.Marshal(&pb.LoginRequest{Username: user, Password: pass, ZoneId: zone})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		fail("login request", err)
	}
	defer resp.Body.Close()
	login := new(pb.LoginResponse)
	_ = json.NewDecoder(resp.Body).Decode(login)
	if login.Code != 0 {
		fail("login response", fmt.Errorf("code=%d msg=%s", login.Code, login.Message))
	}
	return login
}

func gateTCP(gateAddr, token string) {
	conn, err := net.DialTimeout("tcp", gateAddr, 3*time.Second)
	if err != nil {
		fail("gate connect", err)
	}
	defer conn.Close()

	// ping
	writeRead(conn, protocol.Frame{Cmd: protocol.CmdPing, Act: protocol.ActPing, Body: []byte(`{}`)})

	// gate login (session bind)
	writeRead(conn, protocol.Frame{
		Cmd:  protocol.CmdLogin,
		Act:  protocol.ActLogin,
		Body: mustJSON(pb.EnterGateRequest{Token: token}),
	})
	fmt.Println("gate auth ok")
}

func gameFrame(gateAddr, token string) {
	conn, err := net.DialTimeout("tcp", gateAddr, 3*time.Second)
	if err != nil {
		fail("gate reconnect", err)
	}
	defer conn.Close()
	writeRead(conn, protocol.Frame{Cmd: protocol.CmdLogin, Act: protocol.ActLogin, Body: mustJSON(pb.EnterGateRequest{Token: token})})
	body := writeRead(conn, protocol.Frame{Cmd: protocol.CmdGame, Act: protocol.ActPlayerData, Body: []byte(`{"action":"get"}`)})
	fmt.Printf("game frame ok body=%s\n", string(body))
}

func writeRead(conn net.Conn, frame protocol.Frame) []byte {
	if _, err := conn.Write(protocol.Encode(frame)); err != nil {
		fail("gate write", err)
	}
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		fail("gate read header", err)
	}
	size := int(hdr[0])<<8 | int(hdr[1])
	buf := make([]byte, size-2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		fail("gate read body", err)
	}
	decoded, err := protocol.Decode(buf)
	if err != nil {
		fail("gate decode", err)
	}
	return decoded.Body
}

func mustDungeonPass(gameURL string, roleID int64) {
	body, _ := json.Marshal(map[string]any{"role_id": roleID, "dungeon_id": 1})
	resp, err := http.Post(gameURL+"/internal/player/dungeon/pass", "application/json", bytes.NewReader(body))
	if err != nil {
		fail("dungeon pass", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if int(out["code"].(float64)) != 0 {
		fail("dungeon pass", fmt.Errorf("%v", out))
	}
	fmt.Printf("dungeon pass ok level=%v\n", out["level"])
}

func mustRank(url string, roleID int64) {
	resp, err := http.Get(url)
	if err != nil {
		fail("rank", err)
	}
	defer resp.Body.Close()
	var out struct {
		Code int `json:"code"`
		List []struct {
			Member string  `json:"Member"`
			Score  float64 `json:"Score"`
		} `json:"list"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	fmt.Printf("rank ok entries=%d (role %d)\n", len(out.List), roleID)
}

func mustMatch(url string, roleID int64, mode int32) string {
	body, _ := json.Marshal(pb.MatchRequest{RoleId: roleID, Mode: mode, ZoneId: 1})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		fail("match join", err)
	}
	defer resp.Body.Close()
	var out pb.MatchResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Code != 0 {
		fail("match join", fmt.Errorf("code=%d id=%s", out.Code, out.MatchId))
	}
	return out.RoomId
}

func mustBattleSettle(url, roomID string, roleID int64, win bool, score int32) {
	body, _ := json.Marshal(pb.BattleResultRequest{RoomId: roomID, RoleId: roleID, Win: win, Score: score})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		fail("battle settle", err)
	}
	defer resp.Body.Close()
	var out pb.BattleResultResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Code != 0 {
		fail("battle settle", fmt.Errorf("code=%d", out.Code))
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		fail("json", err)
	}
	return b
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(step string, err error) {
	fmt.Fprintf(os.Stderr, "[%s] %v\n", step, err)
	os.Exit(1)
}
