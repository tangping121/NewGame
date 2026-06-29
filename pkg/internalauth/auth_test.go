package internalauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestHTTPMiddlewareEmptySecretPassthrough(t *testing.T) {
	called := false
	h := HTTPMiddleware("", func(w http.ResponseWriter, r *http.Request) { called = true })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/internal/x", nil))
	if !called {
		t.Fatal("empty secret should pass through")
	}
}

func TestHTTPMiddlewareRejectsAndAccepts(t *testing.T) {
	h := HTTPMiddleware("s3cr3t", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	// 无 token -> 403
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/internal/x", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}

	// 正确 token -> 200
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/x", nil)
	SetHTTP(req, "s3cr3t")
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestUnaryInterceptor(t *testing.T) {
	ic := UnaryServerInterceptor("k")
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/x/Y"}

	// 缺 metadata -> 拒绝
	if _, err := ic(context.Background(), nil, info, handler); err == nil {
		t.Fatal("expected unauthenticated error")
	}

	// 带正确 metadata -> 通过
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetaKey, "k"))
	if _, err := ic(ctx, nil, info, handler); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
}
