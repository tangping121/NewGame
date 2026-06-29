package app_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"newgame/pkg/app"
)

func TestWithMetrics(t *testing.T) {
	m := &app.Metrics{}
	h := app.WithMetrics(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), m)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("status %d", rr.Code)
	}
	snap := m.Snapshot()
	if snap["requests"].(uint64) != 1 {
		t.Fatalf("requests %v", snap["requests"])
	}
}
