package rpccodec

import "testing"

func TestJSONCodecRoundTrip(t *testing.T) {
	c := JSON{}
	if c.Name() != Name {
		t.Fatalf("name %s", c.Name())
	}
	type msg struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	in := msg{A: 7, B: "x"}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out msg
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("roundtrip %+v != %+v", out, in)
	}
}
