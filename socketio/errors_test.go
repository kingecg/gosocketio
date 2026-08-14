package socketio

import (
	"errors"
	"testing"
)

// TestClientEmitNeverConnectedNamespace locks the C5b behavior: emitting on a
// namespace the client never connected to returns ErrNamespaceNotConnected,
// which wraps ErrNotConnected so both errors.Is checks hold.
func TestClientEmitNeverConnectedNamespace(t *testing.T) {
	// Given: a client with no connected namespaces.
	c := &Client{}

	// When: emitting on a never-connected namespace.
	err := c.Emit("/never", "event", "arg")

	// Then: the namespace-level sentinel matches, and the base sentinel
	// stays reachable through errors.Is (Metis SHOULD-FIX-2).
	if !errors.Is(err, ErrNamespaceNotConnected) {
		t.Fatalf("errors.Is(err, ErrNamespaceNotConnected) = false, err = %v", err)
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("errors.Is(err, ErrNotConnected) = false, err = %v", err)
	}
}

// TestClientEmitWithAckNeverConnectedNamespace covers the EmitWithAck path,
// which returns (id, err) instead of just err.
func TestClientEmitWithAckNeverConnectedNamespace(t *testing.T) {
	// Given: a client with no connected namespaces.
	c := &Client{}

	// When: emitting with ack on a never-connected namespace.
	id, err := c.EmitWithAck("/never", "event", func(args []any) {}, "arg")

	// Then: no ack id is issued and both sentinel checks hold.
	if id != -1 {
		t.Fatalf("id = %d, want -1", id)
	}
	if !errors.Is(err, ErrNamespaceNotConnected) {
		t.Fatalf("errors.Is(err, ErrNamespaceNotConnected) = false, err = %v", err)
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("errors.Is(err, ErrNotConnected) = false, err = %v", err)
	}
}

// TestErrHandlerMismatchDefined locks the C5b sentinel for handler signature
// mismatches: it is defined, matches itself via errors.Is, and is distinct
// from the not-connected sentinels.
func TestErrHandlerMismatchDefined(t *testing.T) {
	// Given: the exported sentinel.
	// When: matching it against itself and against the other sentinels.
	// Then: it is defined, self-matchable, and distinct.
	if ErrHandlerMismatch == nil {
		t.Fatal("ErrHandlerMismatch is nil")
	}
	if !errors.Is(ErrHandlerMismatch, ErrHandlerMismatch) {
		t.Fatalf("errors.Is(ErrHandlerMismatch, ErrHandlerMismatch) = false")
	}
	if errors.Is(ErrHandlerMismatch, ErrNotConnected) {
		t.Fatal("ErrHandlerMismatch must not match ErrNotConnected")
	}
	if errors.Is(ErrHandlerMismatch, ErrNamespaceNotConnected) {
		t.Fatal("ErrHandlerMismatch must not match ErrNamespaceNotConnected")
	}
}
