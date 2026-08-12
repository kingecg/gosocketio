package transport

import (
	"bytes"
	"testing"
)

func benchPacket() *Packet {
	return &Packet{
		Type: Message,
		Data: []byte(`{"type":"message","data":[{"id":"abc","content":"hello world"}]}`),
	}
}

func BenchmarkEncodePacket(b *testing.B) {
	p := benchPacket()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodePacket(p, true)
	}
}

func BenchmarkDecodePacket(b *testing.B) {
	data := EncodePacket(benchPacket(), true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodePacket(data, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodePayload(b *testing.B) {
	pkts := []*Packet{
		{Type: Message, Data: []byte(`["hello"]`)},
		{Type: Message, Data: []byte(`["world"]`)},
		{Type: Message, Data: []byte(`["foo"]`)},
		{Type: Message, Data: []byte(`["bar"]`)},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodePayload(pkts)
	}
}

func BenchmarkDecodePayload(b *testing.B) {
	pkts := []*Packet{
		{Type: Message, Data: []byte(`["hello"]`)},
		{Type: Message, Data: []byte(`["world"]`)},
		{Type: Message, Data: []byte(`["foo"]`)},
		{Type: Message, Data: []byte(`["bar"]`)},
	}
	data := EncodePayload(pkts)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodePayload(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePayloadBinary(b *testing.B) {
	pkts := []*Packet{
		{Type: Message, Data: []byte(`["hello"]`)},
		{Type: Message, Data: bytes.Repeat([]byte{0xde, 0xad}, 512), IsBinary: true},
	}
	data := EncodePayload(pkts)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodePayload(data); err != nil {
			b.Fatal(err)
		}
	}
}
