package internal

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"newgame/api/pb"

	"go.uber.org/zap"
)

func TestSettleRejectsNonMember(t *testing.T) {
	s := &Server{
		log: zap.NewNop(),
		rooms: map[string]*room{
			"room-1": {
				ID:      "room-1",
				Members: []int64{1, 2},
				Results: make(map[int64]*pb.BattleResultRequest),
				Created: time.Now(),
			},
		},
	}
	body, err := json.Marshal(&pb.BattleResultRequest{RoomId: "room-1", RoleId: 999})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/battle/settle", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handleSettle(rec, req)

	var response pb.BattleResultResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != 1002 {
		t.Fatalf("expected unauthorized code, got %d", response.Code)
	}
	if got := len(s.rooms["room-1"].Results); got != 0 {
		t.Fatalf("non-member result was recorded: %d", got)
	}
}
