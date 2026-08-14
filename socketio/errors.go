package socketio

import (
	"errors"
	"fmt"
)

// sentinel errors exposed to users.
var (
	// ErrNamespaceNotConnected is returned when emitting on a namespace the
	// client is not connected to. It wraps ErrNotConnected so callers can
	// match either the namespace-level or the base sentinel with errors.Is.
	ErrNamespaceNotConnected = fmt.Errorf("%w: namespace not connected", ErrNotConnected)

	// ErrHandlerMismatch is returned when an event handler's signature does
	// not match the event's arguments.
	ErrHandlerMismatch = errors.New("socketio: handler signature mismatch")
)
