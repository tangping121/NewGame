package session

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// Header is the non-browser alternative to Authorization: Bearer <token>.
	Header = "X-Session-Token"
)

type contextKey struct{}

// FromContext returns the identity established by HTTPMiddleware.
func FromContext(ctx context.Context) (Info, bool) {
	info, ok := ctx.Value(contextKey{}).(Info)
	return info, ok && info.RoleID > 0
}

// RoleID returns the trusted role id. Handlers must never fall back to a
// role_id supplied by the request body when this returns zero.
func RoleID(ctx context.Context) int64 {
	info, _ := FromContext(ctx)
	return info.RoleID
}

func tokenFromRequest(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("Authorization")); value != "" {
		const prefix = "Bearer "
		if len(value) > len(prefix) && strings.EqualFold(value[:len(prefix)], prefix) {
			return strings.TrimSpace(value[len(prefix):])
		}
	}
	return strings.TrimSpace(r.Header.Get(Header))
}

// HTTPMiddleware authenticates a login session and stores the immutable
// identity in the request context.
func HTTPMiddleware(rdb goredis.UniversalClient, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rdb == nil {
			writeUnauthorized(w, "session storage unavailable")
			return
		}
		token := tokenFromRequest(r)
		if token == "" {
			writeUnauthorized(w, "session token required")
			return
		}
		info, err := Load(r.Context(), rdb, token)
		if err != nil {
			writeUnauthorized(w, "invalid or expired session")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, info)))
	}
}

func writeUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 1002, "message": message})
}
