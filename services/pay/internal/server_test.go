package internal

import (
	"strings"
	"testing"
)

func TestNewOrderIDIsUniqueAndBounded(t *testing.T) {
	seen := make(map[string]struct{}, 256)
	for i := 0; i < 256; i++ {
		id, err := newOrderID(10001)
		if err != nil {
			t.Fatal(err)
		}
		if len(id) > 64 {
			t.Fatalf("order id exceeds database column: %d", len(id))
		}
		if !strings.HasPrefix(id, "ord_10001_") {
			t.Fatalf("unexpected order id %q", id)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate order id %q", id)
		}
		seen[id] = struct{}{}
	}
}
