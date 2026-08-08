package transport

import "errors"

// Handler receives events from a Transport. It is implemented by the
// Engine.IO session socket.
type Handler interface {
	// OnPacket is called for every packet received from the transport.
	OnPacket(p *Packet)
	// OnError is called when the transport encounters a non-fatal error.
	OnError(err error)
	// OnClose is called when the transport has been closed.
	OnClose()
	// OnDrain is called once all packets handed to Send have been flushed
	// to the wire.
	OnDrain()
	// OnReady is called when the transport becomes writable again, giving
	// the socket a chance to flush buffered packets.
	OnReady()
}

// Transport is a full-duplex Engine.IO transport (polling or websocket).
type Transport interface {
	// Name returns the transport name ("polling" or "websocket").
	Name() string
	// Writable reports whether the transport is ready to accept packets.
	Writable() bool
	// SetWritable toggles the writable flag.
	SetWritable(v bool)
	// Send hands a batch of packets to the transport for delivery. The
	// transport takes ownership of the slice and delivers it in order.
	Send(pkts []*Packet)
	// Close closes the transport. Depending on the transport this may be
	// asynchronous (polling waits for the next poll to deliver a close
	// packet).
	Close()
	// Discard marks the transport as discarded; it will be closed without
	// flushing any remaining data.
	Discard()
	// SetHandler attaches the event handler and starts delivering events.
	SetHandler(h Handler)
}

// Logger is the minimal logging interface used by transports.
type Logger interface {
	Debugf(format string, args ...any)
	Warnf(format string, args ...any)
}

// Sentinel errors surfaced to the session socket.
var (
	ErrRequestOverlap    = errors.New("engineio: request overlap from client")
	ErrDataRequestActive = errors.New("engineio: data request overlap from client")
	ErrPollClosed        = errors.New("engineio: poll connection closed prematurely")
	ErrWriteError        = errors.New("engineio: write error")
	ErrUpgradeTimeout    = errors.New("engineio: upgrade timeout")
	ErrUpgradeAborted    = errors.New("engineio: client did not complete upgrade")
)
