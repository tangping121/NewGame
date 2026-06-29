package protocol_test

import (
	"bytes"
	"io"
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

// TestEncodeExactLength 确认 Encode 不追加多余字节（整帧长度 == 前缀声明的 n）。
func TestEncodeExactLength(t *testing.T) {
	in := protocol.Frame{Cmd: 1, Act: 2, Body: []byte("hello")}
	enc := protocol.Encode(in)
	size := int(enc[0])<<8 | int(enc[1])
	if len(enc) != size {
		t.Fatalf("frame length %d != declared size %d (越界会导致流错乱)", len(enc), size)
	}
	if size != protocol.HeaderSize+len(in.Body) {
		t.Fatalf("size %d unexpected", size)
	}
}

// TestStreamMultiFrame 模拟 Gate 读循环：连续读多帧不应错乱/残留。
func TestStreamMultiFrame(t *testing.T) {
	frames := []protocol.Frame{
		{Cmd: protocol.CmdPing, Act: protocol.ActPing, Body: []byte(`{}`)},
		{Cmd: protocol.CmdLogin, Act: protocol.ActLogin, Body: []byte(`{"token":"t"}`)},
		{Cmd: protocol.CmdGame, Act: protocol.ActPlayerData, Body: []byte(`{"x":123}`)},
	}
	var stream bytes.Buffer
	for _, f := range frames {
		stream.Write(protocol.Encode(f))
	}

	r := bytes.NewReader(stream.Bytes())
	for i, want := range frames {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(r, hdr); err != nil {
			t.Fatalf("frame %d header: %v", i, err)
		}
		size := int(hdr[0])<<8 | int(hdr[1])
		body := make([]byte, size)
		copy(body[0:2], hdr)
		if _, err := io.ReadFull(r, body[2:]); err != nil {
			t.Fatalf("frame %d body: %v", i, err)
		}
		got, err := protocol.Decode(body[2:])
		if err != nil {
			t.Fatalf("frame %d decode: %v", i, err)
		}
		if got.Cmd != want.Cmd || got.Act != want.Act || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("frame %d mismatch: got %+v want %+v", i, got, want)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("stream has %d leftover bytes (帧错乱)", r.Len())
	}
}
