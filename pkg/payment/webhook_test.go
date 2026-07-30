package payment

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSignDeterministic(t *testing.T) {
	const expected = "51cea6ef3396d59ff74c9d16ae8e3fe878477767ee1c0b5e8236b88ce415dd00"
	if got := Sign("secret", "1700000000", "nonce", []byte(`{"ok":true}`)); got != expected {
		t.Fatalf("signature %s", got)
	}
}

func TestWebhookRejectsOversizedSignatureBeforeRedis(t *testing.T) {
	handler := VerifyWebhook("secret", 0, nil, func(http.ResponseWriter, *http.Request) {
		t.Fatal("callback invoked")
	})
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	request.Header.Set(TimestampHeader, "1700000000")
	request.Header.Set(NonceHeader, "nonce")
	request.Header.Set(SignatureHeader, strings.Repeat("a", 65))
	response := httptest.NewRecorder()
	handler(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", response.Code)
	}
}
