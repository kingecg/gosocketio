package transport

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeTextPacket(t *testing.T) {
	cases := []struct {
		name string
		p    Packet
		want string
	}{
		{"open", Packet{Type: Open, Data: []byte(`{"sid":"x"}`)}, `0{"sid":"x"}`},
		{"ping", Packet{Type: Ping}, "2"},
		{"pong probe", Packet{Type: Pong, Data: []byte("probe")}, "3probe"},
		{"message", Packet{Type: Message, Data: []byte("hello")}, "4hello"},
		{"upgrade", Packet{Type: Upgrade}, "5"},
		{"noop", Packet{Type: Noop}, "6"},
		{"close", Packet{Type: Close}, "1"},
		{"empty message", Packet{Type: Message}, "4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := EncodePacket(&tc.p, true)
			if string(enc) != tc.want {
				t.Fatalf("EncodePacket = %q, want %q", enc, tc.want)
			}
			dec, err := DecodePacket(enc, false)
			if err != nil {
				t.Fatalf("DecodePacket: %v", err)
			}
			if dec.Type != tc.p.Type {
				t.Fatalf("type = %v, want %v", dec.Type, tc.p.Type)
			}
			if !bytes.Equal(dec.Data, tc.p.Data) {
				t.Fatalf("data = %q, want %q", dec.Data, tc.p.Data)
			}
		})
	}
}

func TestEncodeBinaryWebsocket(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	p := Packet{Type: Message, Data: data, IsBinary: true}
	enc := EncodePacket(&p, true)
	if !bytes.Equal(enc, data) {
		t.Fatalf("websocket binary encoding = %v, want raw %v", enc, data)
	}
	dec, err := DecodePacket(enc, true)
	if err != nil {
		t.Fatalf("DecodePacket(binary): %v", err)
	}
	if dec.Type != Message || !dec.IsBinary || !bytes.Equal(dec.Data, data) {
		t.Fatalf("decoded = %+v, want message packet with %v", dec, data)
	}
}

func TestEncodeBinaryPolling(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	p := Packet{Type: Message, Data: data, IsBinary: true}
	enc := EncodePacket(&p, false)
	want := "bAQIDBA=="
	if string(enc) != want {
		t.Fatalf("polling binary encoding = %q, want %q", enc, want)
	}
	dec, err := DecodePacket(enc, false)
	if err != nil {
		t.Fatalf("DecodePacket: %v", err)
	}
	if dec.Type != Message || !dec.IsBinary || !bytes.Equal(dec.Data, data) {
		t.Fatalf("decoded = %+v, want message packet with %v", dec, data)
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, err := DecodePacket(nil, false); err == nil {
		t.Fatal("expected error for empty input")
	}
	if _, err := DecodePacket([]byte("zwhatever"), false); err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestEncodeDecodePayload(t *testing.T) {
	pkts := []*Packet{
		{Type: Message, Data: []byte("hello")},
		{Type: Ping},
		{Type: Message, Data: []byte("world")},
	}
	enc := EncodePayload(pkts)
	want := "4hello\x1e2\x1e4world"
	if string(enc) != want {
		t.Fatalf("EncodePayload = %q, want %q", enc, want)
	}
	dec, err := DecodePayload(enc)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(dec) != 3 {
		t.Fatalf("decoded %d packets, want 3", len(dec))
	}
	if string(dec[0].Data) != "hello" || dec[1].Type != Ping || string(dec[2].Data) != "world" {
		t.Fatalf("unexpected packets: %+v", dec)
	}
}

func TestEncodeDecodePayloadWithBinary(t *testing.T) {
	pkts := []*Packet{
		{Type: Message, Data: []byte("hello")},
		{Type: Message, Data: []byte{0xde, 0xad}, IsBinary: true},
	}
	enc := EncodePayload(pkts)
	want := "4hello\x1eb3q0="
	if string(enc) != want {
		t.Fatalf("EncodePayload = %q, want %q", enc, want)
	}
	dec, err := DecodePayload(enc)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(dec) != 2 {
		t.Fatalf("decoded %d packets, want 2", len(dec))
	}
	if !dec[1].IsBinary || !bytes.Equal(dec[1].Data, []byte{0xde, 0xad}) {
		t.Fatalf("binary packet mismatch: %+v", dec[1])
	}
}

func TestDecodePayloadEmpty(t *testing.T) {
	pkts, err := DecodePayload([]byte{})
	if err != nil {
		t.Fatalf("DecodePayload(empty): %v", err)
	}
	if len(pkts) != 0 {
		t.Fatalf("expected 0 packets, got %d", len(pkts))
	}
}
