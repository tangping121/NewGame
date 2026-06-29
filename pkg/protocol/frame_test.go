package protocol_test

import (
	"bytes"
	"testing"

	"newgame/pkg/protocol"
)

func TestEncodeDecode(t *testing.T) {
	in := protocol.Frame{Cmd: protocol.CmdGame, Act: protocol.ActPlayerData, Body: []byte(`{"a":1}`)}
	enc := protocol.Encode(in)
	size := int(enc[0])<<8 | int(enc[1])
	out, err := protocol.Decode(enc[2 : 2+size-2])
	if err != nil {
		t.Fatal(err)
	}
	if out.Cmd != in.Cmd || out.Act != in.Act || !bytes.Equal(out.Body, in.Body) {
		t.Fatalf("roundtrip mismatch: %+v", out)
	}
}
