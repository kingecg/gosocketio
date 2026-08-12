package socketio

import "testing"

func benchEventPacket() *Packet {
	return &Packet{
		Type: Event,
		Nsp:  "/",
		ID:   7,
		Data: []any{"message", "hello world", map[string]any{"user": "alice", "count": 3}},
	}
}

func BenchmarkEncodePacket(b *testing.B) {
	p := benchEventPacket()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Encode()
	}
}

func BenchmarkDecodePacket(b *testing.B) {
	data := benchEventPacket().Encode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeConnect(b *testing.B) {
	data := []byte(`0{"sid":"abc123","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}
