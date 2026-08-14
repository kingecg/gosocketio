package engineio

import (
	"errors"
	"fmt"

	"github.com/kingecg/gosocketio/engineio/transport"
)

// Protocol error codes, mirroring the reference implementation.
const (
	ErrCodeUnknownTransport = iota
	ErrCodeUnknownSID
	ErrCodeBadHandshakeMethod
	ErrCodeBadRequest
	ErrCodeForbidden
	ErrCodeUnsupportedProtocolVersion
)

// Sentinel errors.
var (
	// ErrUnsupportedProtocol is returned when the EIO protocol version is
	// not 4.
	ErrUnsupportedProtocol = errors.New("engineio: unsupported protocol version")

	// ErrUnknownTransport is returned when the transport is not enabled.
	ErrUnknownTransport = errors.New("engineio: unknown transport")

	// ErrBadHandshakeMethod is returned when a handshake request is not GET.
	ErrBadHandshakeMethod = errors.New("engineio: bad handshake method")

	// ErrBadRequest is returned for generic bad requests.
	ErrBadRequest = errors.New("engineio: bad request")

	// ErrForbidden is returned when AllowRequest rejects the handshake.
	ErrForbidden = errors.New("engineio: forbidden")

	// ErrUnknownSID is returned when the sid is not known.
	ErrUnknownSID = errors.New("engineio: session id unknown")

	// ErrInvalidPacket is returned when a transport packet cannot be
	// decoded. It wraps transport.ErrInvalidPacket, so errors.Is matches
	// both this sentinel and the transport-level one.
	ErrInvalidPacket = fmt.Errorf("%w: invalid packet", transport.ErrInvalidPacket)

	// ErrHeartbeatTimeout is returned when the peer fails to answer a ping
	// within the configured ping timeout.
	ErrHeartbeatTimeout = errors.New("engineio: heartbeat timeout")
)

// wrapInvalidPacket rewraps a transport packet-decode error as
// engineio.ErrInvalidPacket so that the engineio-level close surfaces the
// engineio sentinel; other transport errors pass through unchanged.
func wrapInvalidPacket(err error) error {
	if errors.Is(err, transport.ErrInvalidPacket) {
		return fmt.Errorf("%w: %v", ErrInvalidPacket, err)
	}
	return err
}
