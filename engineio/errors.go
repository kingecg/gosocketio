package engineio

import "errors"

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
)
