// Package transport implements the Engine.IO v4 protocol primitives
// (packet/payload encoding) and the underlying HTTP long-polling and
// WebSocket transports.
//
// Protocol reference: https://socket.io/docs/v4/engine-io-protocol/
package transport

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
)

// Type is the Engine.IO packet type.
type Type byte

// Engine.IO packet types.
const (
	Open    Type = '0'
	Close   Type = '1'
	Ping    Type = '2'
	Pong    Type = '3'
	Message Type = '4'
	Upgrade Type = '5'
	Noop    Type = '6'
)

// String returns the packet type name.
func (t Type) String() string {
	switch t {
	case Open:
		return "open"
	case Close:
		return "close"
	case Ping:
		return "ping"
	case Pong:
		return "pong"
	case Message:
		return "message"
	case Upgrade:
		return "upgrade"
	case Noop:
		return "noop"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// Packet is a single Engine.IO packet.
type Packet struct {
	// Type is the packet type. A packet decoded from a binary source always
	// has Type == Message.
	Type Type
	// Data is the packet payload. For a text packet it holds the raw UTF-8
	// data. For a binary packet it holds the binary payload.
	Data []byte
	// IsBinary reports whether Data holds binary data. When true the packet
	// is a message packet carrying binary data.
	IsBinary bool
}

// ErrInvalidPacket is returned when a packet cannot be decoded.
var ErrInvalidPacket = errors.New("engineio: invalid packet")

// ErrInvalidPayload is returned when a polling payload cannot be decoded.
var ErrInvalidPayload = errors.New("engineio: invalid payload")

// ErrPayloadTooLarge is returned when a single packet within a polling
// payload exceeds the configured maxPayload. It is treated as fatal: the
// caller closes the connection (the payload cannot be partially honored).
var ErrPayloadTooLarge = errors.New("engineio: payload too large")

// packageLogger is used by DecodePayloadWithLimit to warn about payloads
// truncated at the cumulative maxPayload limit. It defaults to discarding
// output; override it with SetLogger. It is guarded by loggerMu because
// transports may configure it concurrently during handshakes.
var (
	loggerMu      sync.RWMutex
	packageLogger Logger = nopLogger{}
)

// SetLogger sets the package-level logger used for payload-truncation
// warnings. A nil logger leaves the current logger in place.
func SetLogger(l Logger) {
	if l == nil {
		return
	}
	loggerMu.Lock()
	packageLogger = l
	loggerMu.Unlock()
}

// warnf logs through the package-level logger.
func warnf(format string, args ...any) {
	loggerMu.RLock()
	l := packageLogger
	loggerMu.RUnlock()
	l.Warnf(format, args...)
}

// nopLogger discards all log output.
type nopLogger struct{}

func (nopLogger) Debugf(format string, args ...any) {}
func (nopLogger) Warnf(format string, args ...any)  {}

// EncodePacket serializes a packet.
//
// When the packet carries binary data and supportsBinary is true, the raw
// bytes are returned unchanged (the packet type is implicitly "message",
// which matches the WebSocket transport). Otherwise binary data is
// base64-encoded and prefixed with a 'b' character, as required by the
// HTTP long-polling transport.
func EncodePacket(p *Packet, supportsBinary bool) []byte {
	if p.IsBinary {
		if supportsBinary {
			return p.Data
		}
		n := base64.StdEncoding.EncodedLen(len(p.Data))
		out := make([]byte, 1, 1+n)
		out[0] = 'b'
		return append(out, base64.StdEncoding.EncodeToString(p.Data)...)
	}
	out := make([]byte, 0, 1+len(p.Data))
	out = append(out, byte(p.Type))
	out = append(out, p.Data...)
	return out
}

// DecodePacket parses a single packet.
//
// isBinary indicates the input arrived as a binary frame (e.g. a binary
// WebSocket frame); such a frame is always a message packet carrying binary
// data. Text input may be:
//
//	"<type><data>"          plain text packet
//	"b<base64>"             binary message payload (base64 encoded)
func DecodePacket(b []byte, isBinary bool) (Packet, error) {
	if isBinary {
		return Packet{Type: Message, Data: b, IsBinary: true}, nil
	}
	if len(b) == 0 {
		return Packet{}, ErrInvalidPacket
	}
	c := b[0]
	if c == 'b' {
		n := base64.StdEncoding.DecodedLen(len(b) - 1)
		data := make([]byte, n)
		m, err := base64.StdEncoding.Decode(data, b[1:])
		if err != nil {
			return Packet{}, fmt.Errorf("%w: %v", ErrInvalidPacket, err)
		}
		return Packet{Type: Message, Data: data[:m], IsBinary: true}, nil
	}
	t := Type(c)
	if t < Open || t > Noop {
		return Packet{}, fmt.Errorf("%w: unknown type %q", ErrInvalidPacket, c)
	}
	p := Packet{Type: t}
	if len(b) > 1 {
		p.Data = b[1:]
	}
	return p, nil
}

// separator is the record separator character (0x1e) used to join packets
// in an HTTP long-polling payload.
const separator byte = 0x1e

// EncodePayload serializes a batch of packets into a polling payload.
// Binary packets are always base64-encoded, as required by the protocol.
func EncodePayload(pkts []*Packet) []byte {
	var buf bytes.Buffer
	for i, p := range pkts {
		if i > 0 {
			buf.WriteByte(separator)
		}
		buf.Write(EncodePacket(p, false))
	}
	return buf.Bytes()
}

// DecodePayload parses a polling payload into its packets. The input must be
// plain text (the v4 protocol never sends raw binary in polling payloads).
func DecodePayload(b []byte) ([]*Packet, error) {
	return DecodePayloadWithLimit(b, 0)
}

// DecodePayloadWithLimit parses a polling payload into its packets while
// enforcing a maximum size. A maxPayload <= 0 disables the limit entirely.
//
// A single packet whose own encoded size exceeds maxPayload is fatal and
// returns an error wrapping ErrPayloadTooLarge; the caller must close the
// connection. When the cumulative size of the packets decoded so far would
// exceed maxPayload, the remaining tail packets (the newest) are dropped
// with a warning instead — the payload is recoverable and the connection
// stays open.
func DecodePayloadWithLimit(b []byte, maxPayload int64) ([]*Packet, error) {
	if len(b) == 0 {
		return nil, nil
	}
	parts := bytes.Split(b, []byte{separator})
	out := make([]*Packet, 0, len(parts))
	var decoded int64
	for i, part := range parts {
		if maxPayload > 0 {
			size := int64(len(part))
			if size > maxPayload {
				return out, fmt.Errorf("%w: packet %d bytes exceeds maxPayload %d", ErrPayloadTooLarge, size, maxPayload)
			}
			if decoded+size > maxPayload {
				dropped := int64(len(parts) - i)
				warnf("engineio: dropping %d packet(s) beyond maxPayload %d (cumulative %d bytes)", dropped, maxPayload, decoded)
				break
			}
			decoded += size
		}
		p, err := DecodePacket(part, false)
		if err != nil {
			return out, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		out = append(out, &p)
	}
	return out, nil
}
