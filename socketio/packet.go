// Package socketio implements the Socket.IO v5 protocol layer on top of the
// engineio transport package.
//
// Protocol reference: https://socket.io/docs/v4/socket-io-protocol/
package socketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

// PacketType is the Socket.IO packet type.
type PacketType byte

// Socket.IO packet types.
const (
	Connect      PacketType = '0'
	Disconnect   PacketType = '1'
	Event        PacketType = '2'
	Ack          PacketType = '3'
	ConnectError PacketType = '4'
	BinaryEvent  PacketType = '5'
	BinaryAck    PacketType = '6'
)

// String returns the packet type name.
func (t PacketType) String() string {
	switch t {
	case Connect:
		return "connect"
	case Disconnect:
		return "disconnect"
	case Event:
		return "event"
	case Ack:
		return "ack"
	case ConnectError:
		return "connect_error"
	case BinaryEvent:
		return "binary_event"
	case BinaryAck:
		return "binary_ack"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// defaultNamespace is the root namespace. It is omitted from the wire format.
const defaultNamespace = "/"

// Packet is a Socket.IO packet. Data holds the decoded JSON payload:
//   - Connect / ConnectError: map[string]any (or nil)
//   - Event: []any ([eventName, args...])
//   - Ack: []any ([args...])
//
// A negative ID means the packet carries no acknowledgement id.
type Packet struct {
	Type PacketType
	Nsp  string
	ID   int64
	Data any

	// Attachments is the number of binary buffers following a binary
	// packet. It is only meaningful for BinaryEvent/BinaryAck packets.
	Attachments int
}

// Encode serializes the packet to its text form. Binary packets must have
// already been deconstructed into placeholder form (see deconstruct).
func (p *Packet) Encode() []byte {
	b := []byte{byte(p.Type)}
	if p.Type == BinaryEvent || p.Type == BinaryAck {
		b = append(b, strconv.Itoa(p.Attachments)...)
		b = append(b, '-')
	}
	if p.Nsp != "" && p.Nsp != defaultNamespace {
		b = append(b, p.Nsp...)
		b = append(b, ',')
	}
	if p.ID >= 0 {
		b = append(b, strconv.FormatInt(p.ID, 10)...)
	}
	if p.Data != nil {
		if j, err := json.Marshal(p.Data); err == nil {
			b = append(b, j...)
		}
	}
	return b
}

// Decode parses a text Socket.IO packet.
func Decode(data []byte) (*Packet, error) {
	if len(data) == 0 {
		return nil, errors.New("socketio: empty packet")
	}
	p := &Packet{Type: PacketType(data[0]), Nsp: defaultNamespace, ID: -1}
	switch p.Type {
	case Connect, Disconnect, Event, Ack, ConnectError, BinaryEvent, BinaryAck:
	default:
		return nil, fmt.Errorf("socketio: unknown packet type %q", data[0])
	}

	i := 0
	// attachments (binary packets only)
	if p.Type == BinaryEvent || p.Type == BinaryAck {
		j := i + 1
		for j < len(data) && data[j] != '-' {
			j++
		}
		if j == len(data) {
			return nil, errors.New("socketio: illegal attachments")
		}
		n, err := strconv.Atoi(string(data[i+1 : j]))
		if err != nil || n < 0 {
			return nil, errors.New("socketio: illegal attachments")
		}
		p.Attachments = n
		i = j // i points at '-'
	}

	// namespace
	if i+1 < len(data) && data[i+1] == '/' {
		j := i + 1
		for j < len(data) && data[j] != ',' {
			j++
		}
		p.Nsp = string(data[i+1 : j])
		if j < len(data) {
			i = j // i points at ','
		} else {
			i = len(data) // no data follows
		}
	}

	// acknowledgement id
	if i+1 < len(data) && isDigit(data[i+1]) {
		j := i + 1
		for j < len(data) && isDigit(data[j]) {
			j++
		}
		p.ID, _ = strconv.ParseInt(string(data[i+1:j]), 10, 64)
		i = j - 1
	}

	// payload
	if i+1 < len(data) {
		if err := json.Unmarshal(data[i+1:], &p.Data); err != nil {
			return nil, fmt.Errorf("socketio: invalid payload: %w", err)
		}
		if err := validatePayload(p.Type, p.Data); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// reservedEvents cannot be received as event names, mirroring the reference
// implementation.
var reservedEvents = map[string]bool{
	"connect": true, "connect_error": true, "disconnect": true,
	"disconnecting": true, "newListener": true, "removeListener": true,
}

func isReservedEvent(name string) bool { return reservedEvents[name] }

// validatePayload enforces the same constraints as the official parser.
func validatePayload(t PacketType, data any) error {
	switch t {
	case Connect:
		if data != nil {
			if _, ok := data.(map[string]any); !ok {
				return errors.New("socketio: invalid connect payload")
			}
		}
	case Disconnect:
		if data != nil {
			return errors.New("socketio: invalid disconnect payload")
		}
	case ConnectError:
		if data != nil {
			switch data.(type) {
			case string, map[string]any:
			default:
				return errors.New("socketio: invalid connect_error payload")
			}
		}
	case Event, BinaryEvent:
		arr, ok := data.([]any)
		if !ok || len(arr) == 0 {
			return errors.New("socketio: invalid event payload")
		}
		switch first := arr[0].(type) {
		case float64:
			// a numeric event name is tolerated but never dispatched
		case string:
			if isReservedEvent(first) {
				return errors.New("socketio: reserved event name")
			}
		default:
			return errors.New("socketio: invalid event name")
		}
	case Ack, BinaryAck:
		if _, ok := data.([]any); !ok {
			return errors.New("socketio: invalid ack payload")
		}
	}
	return nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// placeholder mirrors the official `{"_placeholder":true,"num":N}` marker.
type placeholder struct {
	Placeholder bool `json:"_placeholder"`
	Num         int  `json:"num"`
}

// hasBinary reports whether v contains any []byte or io.Reader values.
func hasBinary(v any) bool {
	switch x := v.(type) {
	case []byte, io.Reader:
		return true
	case []any:
		for _, e := range x {
			if hasBinary(e) {
				return true
			}
		}
	case map[string]any:
		for _, e := range x {
			if hasBinary(e) {
				return true
			}
		}
	}
	return false
}

// deconstruct walks v replacing every []byte and io.Reader with a placeholder
// and appends the raw buffers to bufs in order. io.Reader values are read to
// EOF exactly once at encode time; a read failure returns an error and leaves
// bufs untouched for that value.
func deconstruct(v any, bufs *[][]byte) (any, error) {
	switch x := v.(type) {
	case []byte:
		num := len(*bufs)
		*bufs = append(*bufs, x)
		return placeholder{Placeholder: true, Num: num}, nil
	case io.Reader:
		b, err := readAll(x)
		if err != nil {
			return nil, fmt.Errorf("socketio: reading binary payload: %w", err)
		}
		num := len(*bufs)
		*bufs = append(*bufs, b)
		return placeholder{Placeholder: true, Num: num}, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			d, err := deconstruct(e, bufs)
			if err != nil {
				return nil, err
			}
			out[i] = d
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			d, err := deconstruct(e, bufs)
			if err != nil {
				return nil, err
			}
			out[k] = d
		}
		return out, nil
	default:
		return v, nil
	}
}

// readAll drains r to EOF. A nil reader (including a typed nil such as
// (*bytes.Reader)(nil), whose Read method would panic) is treated as an empty
// payload.
func readAll(r io.Reader) ([]byte, error) {
	if r == nil {
		return []byte{}, nil
	}
	switch rv := reflect.ValueOf(r); rv.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface:
		if rv.IsNil() {
			return []byte{}, nil
		}
	}
	return io.ReadAll(r)
}

// reconstruct walks v (as decoded from JSON) replacing placeholders with the
// corresponding binary buffers.
func reconstruct(v any, bufs [][]byte) any {
	switch x := v.(type) {
	case map[string]any:
		if b, ok := x["_placeholder"].(bool); ok && b {
			if num, ok := x["num"].(float64); ok {
				idx := int(num)
				if idx >= 0 && idx < len(bufs) {
					return bufs[idx]
				}
			}
			return nil
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = reconstruct(e, bufs)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = reconstruct(e, bufs)
		}
		return out
	default:
		return v
	}
}
