package discovery

import "testing"

func TestPublishAddr(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{":9100", "127.0.0.1:9100"},
		{"0.0.0.0:9000", "127.0.0.1:9000"},
		{"192.168.1.10:8080", "192.168.1.10:8080"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := PublishAddr(tt.in); got != tt.want {
			t.Fatalf("PublishAddr(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestInstanceHTTPBase(t *testing.T) {
	inst := Instance{HTTPAddr: "127.0.0.1:9100"}
	if got := inst.HTTPBase(); got != "http://127.0.0.1:9100" {
		t.Fatalf("HTTPBase() = %q", got)
	}
}
