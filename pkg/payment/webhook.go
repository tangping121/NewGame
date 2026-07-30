// Package payment contains payment-provider boundary security.
package payment

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	TimestampHeader = "X-Payment-Timestamp"
	NonceHeader     = "X-Payment-Nonce"
	SignatureHeader = "X-Payment-Signature"
)

// Sign returns the lowercase hex HMAC used by payment providers and tests.
func Sign(secret, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyWebhook validates freshness, HMAC, and one-time nonce before invoking
// the callback handler. The exact request body is restored for JSON decoding.
func VerifyWebhook(secret string, maxSkew time.Duration, rdb goredis.UniversalClient, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if secret == "" {
			writeError(w, http.StatusServiceUnavailable, "payment webhook is not configured")
			return
		}
		timestamp := strings.TrimSpace(r.Header.Get(TimestampHeader))
		nonce := strings.TrimSpace(r.Header.Get(NonceHeader))
		signature := strings.TrimSpace(r.Header.Get(SignatureHeader))
		if len(timestamp) > 20 || len(nonce) == 0 || len(nonce) > 128 || len(signature) != sha256.Size*2 {
			writeError(w, http.StatusUnauthorized, "invalid payment signature headers")
			return
		}
		unix, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil || nonce == "" || signature == "" {
			writeError(w, http.StatusUnauthorized, "invalid payment signature headers")
			return
		}
		if maxSkew <= 0 {
			maxSkew = 5 * time.Minute
		}
		delta := time.Since(time.Unix(unix, 0))
		if delta < -maxSkew || delta > maxSkew {
			writeError(w, http.StatusUnauthorized, "stale payment callback")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid payment callback body")
			return
		}
		expected, err := hex.DecodeString(Sign(secret, timestamp, nonce, body))
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid payment signature")
			return
		}
		got, err := hex.DecodeString(signature)
		if err != nil || !hmac.Equal(got, expected) {
			writeError(w, http.StatusUnauthorized, "invalid payment signature")
			return
		}
		if rdb == nil {
			writeError(w, http.StatusServiceUnavailable, "payment replay protection unavailable")
			return
		}
		replayKey := fmt.Sprintf("ng:pay:{webhook}:nonce:%x", sha256.Sum256([]byte(nonce)))
		ok, err := rdb.SetNX(r.Context(), replayKey, timestamp, 2*maxSkew).Result()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "payment replay protection unavailable")
			return
		}
		if !ok {
			writeError(w, http.StatusConflict, "payment callback replayed")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 1002, "message": message})
}
