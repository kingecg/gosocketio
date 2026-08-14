package transport

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
)

// captureLogger implements Logger and records every Warnf call.
type captureLogger struct {
	mu   sync.Mutex
	warn []string
}

func (c *captureLogger) Debugf(format string, args ...any) {}
func (c *captureLogger) Warnf(format string, args ...any) {
	c.mu.Lock()
	c.warn = append(c.warn, format)
	c.mu.Unlock()
}

func (c *captureLogger) warns() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.warn...)
}

// samplePayload returns the wire encoding of three text packets whose
// individual encoded sizes are all small enough to be under any limit used
// in these tests.
func samplePayload() []byte {
	return EncodePayload([]*Packet{
		{Type: Message, Data: []byte("hello")}, // "4hello"   = 6 bytes
		{Type: Ping},                           // "2"        = 1 byte
		{Type: Message, Data: []byte("world")}, // "4world"   = 6 bytes
	})
}

func TestDecodePayloadWithLimitNoLimit(t *testing.T) {
	// Given: a payload of three packets and a limit of zero / negative.
	b := samplePayload()

	// When: the payload is decoded with maxPayload <= 0 (no limit).
	// Then: the result is identical to DecodePayload (unlimited).
	want, err := DecodePayload(b)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	for _, maxPayload := range []int64{0, -1, -100} {
		got, err := DecodePayloadWithLimit(b, maxPayload)
		if err != nil {
			t.Fatalf("DecodePayloadWithLimit(%d): %v", maxPayload, err)
		}
		if len(got) != len(want) {
			t.Fatalf("maxPayload=%d: got %d packets, want %d", maxPayload, len(got), len(want))
		}
		for i := range want {
			if got[i].Type != want[i].Type || !bytes.Equal(got[i].Data, want[i].Data) || got[i].IsBinary != want[i].IsBinary {
				t.Fatalf("maxPayload=%d: packet %d = %+v, want %+v", maxPayload, i, got[i], want[i])
			}
		}
	}
}

func TestDecodePayloadWithLimitDropsTailPackets(t *testing.T) {
	// Given: a payload of three packets ("4hello" 6B, "2" 1B, "4world" 6B)
	// and a limit of 7 bytes, which admits the first two packets but not the
	// third.
	b := samplePayload()
	cl := &captureLogger{}
	SetLogger(cl)

	// When: the payload is decoded with maxPayload=7.
	got, err := DecodePayloadWithLimit(b, 7)

	// Then: decoding succeeds (recoverable — the connection stays open), the
	// first two packets survive, the tail packet is dropped, and a warning
	// was logged.
	if err != nil {
		t.Fatalf("DecodePayloadWithLimit: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d packets, want 2 (tail dropped)", len(got))
	}
	if string(got[0].Data) != "hello" {
		t.Fatalf("packet 0 = %q, want %q", got[0].Data, "hello")
	}
	if got[1].Type != Ping {
		t.Fatalf("packet 1 = %+v, want ping", got[1])
	}
	if warns := cl.warns(); len(warns) == 0 {
		t.Fatal("expected a warn log for dropped tail packets, got none")
	}
}

func TestDecodePayloadWithLimitNoDropWithinLimit(t *testing.T) {
	// Given: a payload of three packets totalling 13 bytes and a limit of 13.
	b := samplePayload()
	cl := &captureLogger{}
	SetLogger(cl)

	// When: the cumulative size exactly equals maxPayload.
	got, err := DecodePayloadWithLimit(b, 13)

	// Then: every packet is kept (the limit is not exceeded) and nothing is
	// logged.
	if err != nil {
		t.Fatalf("DecodePayloadWithLimit: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d packets, want 3", len(got))
	}
	if warns := cl.warns(); len(warns) != 0 {
		t.Fatalf("expected no warnings, got %v", warns)
	}
}

func TestDecodePayloadWithLimitSingleOversized(t *testing.T) {
	// Given: a single packet whose encoded size (9 bytes) exceeds maxPayload.
	b := []byte("4oversize") // "4" + 8 bytes payload

	// When: the payload is decoded with maxPayload=8.
	_, err := DecodePayloadWithLimit(b, 8)

	// Then: an error wrapping ErrPayloadTooLarge is returned (the caller
	// closes the connection).
	if err == nil {
		t.Fatal("expected error for oversized single packet")
	}
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("err = %v, want errors.Is(err, ErrPayloadTooLarge)", err)
	}
	if !strings.Contains(err.Error(), "9") || !strings.Contains(err.Error(), "8") {
		t.Fatalf("err = %q, want it to report packet bytes and maxPayload", err)
	}
}

func TestDecodePayloadWithLimitOversizedInBatch(t *testing.T) {
	// Given: a batch whose middle packet (9 bytes) exceeds maxPayload, with
	// earlier packets already decoded.
	b := EncodePayload([]*Packet{
		{Type: Message, Data: []byte("a")},        // "4a" = 2 bytes
		{Type: Message, Data: []byte("oversize")}, // "4oversize" = 9 bytes
		{Type: Message, Data: []byte("b")},        // "4b" = 2 bytes
	})

	// When: decoded with maxPayload=8.
	_, err := DecodePayloadWithLimit(b, 8)

	// Then: ErrPayloadTooLarge is returned regardless of the packets decoded
	// so far — a single oversized packet is fatal.
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("err = %v, want errors.Is(err, ErrPayloadTooLarge)", err)
	}
}

func TestDecodePayloadWithLimitEmpty(t *testing.T) {
	// Given: an empty payload.
	// When: decoded with any limit.
	// Then: no packets and no error.
	for _, maxPayload := range []int64{0, 100} {
		got, err := DecodePayloadWithLimit([]byte{}, maxPayload)
		if err != nil {
			t.Fatalf("DecodePayloadWithLimit(%d): %v", maxPayload, err)
		}
		if len(got) != 0 {
			t.Fatalf("got %d packets, want 0", len(got))
		}
	}
}
