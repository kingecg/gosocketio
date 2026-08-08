package transport

import (
	"context"
	"sync"

	"github.com/coder/websocket"
)

// Websocket is the WebSocket transport. Each Engine.IO packet is carried in
// its own WebSocket frame: plain text packets use text frames
// ("<type><data>") while binary message payloads use binary frames carrying
// the raw bytes.
type Websocket struct {
	ctx    context.Context
	cancel context.CancelFunc

	conn *websocket.Conn

	mu        sync.Mutex
	handler   Handler
	logger    Logger
	writable  bool
	discarded bool
	closed    bool
	readOnce  sync.Once
}

// NewWebsocket wraps an accepted WebSocket connection.
func NewWebsocket(ctx context.Context, conn *websocket.Conn) *Websocket {
	c, cancel := context.WithCancel(ctx)
	return &Websocket{
		ctx:      c,
		cancel:   cancel,
		conn:     conn,
		writable: true,
	}
}

// SetLogger sets the logger used by the transport.
func (w *Websocket) SetLogger(l Logger) {
	w.mu.Lock()
	w.logger = l
	w.mu.Unlock()
}

func (w *Websocket) Name() string { return "websocket" }

func (w *Websocket) Writable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writable
}

func (w *Websocket) SetWritable(v bool) {
	w.mu.Lock()
	w.writable = v
	w.mu.Unlock()
}

func (w *Websocket) SetHandler(h Handler) {
	w.mu.Lock()
	w.handler = h
	w.mu.Unlock()
	w.readOnce.Do(func() { go w.readLoop() })
}

func (w *Websocket) readLoop() {
	defer func() {
		// Surface close only for non-discarded transports; the session
		// socket ignores events from discarded transports anyway.
		w.mu.Lock()
		h := w.handler
		w.mu.Unlock()
		if h != nil {
			h.OnClose()
		}
	}()
	for {
		typ, data, err := w.conn.Read(w.ctx)
		if err != nil {
			return
		}
		var p Packet
		if typ == websocket.MessageBinary {
			p = Packet{Type: Message, Data: data, IsBinary: true}
		} else {
			decoded, derr := DecodePacket(data, false)
			if derr != nil {
				w.mu.Lock()
				h := w.handler
				w.mu.Unlock()
				if h != nil {
					h.OnError(derr)
				}
				continue
			}
			p = decoded
		}
		w.mu.Lock()
		h := w.handler
		w.mu.Unlock()
		if h != nil {
			h.OnPacket(&p)
		}
	}
}

// Send delivers a batch of packets over the WebSocket connection.
func (w *Websocket) Send(pkts []*Packet) {
	if len(pkts) == 0 {
		return
	}
	for i, p := range pkts {
		if p.IsBinary {
			if err := w.conn.Write(w.ctx, websocket.MessageBinary, p.Data); err != nil {
				w.notifyError(err)
				return
			}
		} else {
			if err := w.conn.Write(w.ctx, websocket.MessageText, EncodePacket(p, true)); err != nil {
				w.notifyError(err)
				return
			}
		}
		_ = i
	}
	w.mu.Lock()
	w.writable = true
	h := w.handler
	w.mu.Unlock()
	if h != nil {
		h.OnDrain()
		h.OnReady()
	}
}

// Discard marks the transport as discarded; subsequent events are ignored by
// the session socket.
func (w *Websocket) Discard() {
	w.mu.Lock()
	w.discarded = true
	w.mu.Unlock()
}

// Close closes the underlying WebSocket connection.
func (w *Websocket) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	_ = w.conn.Close(websocket.StatusNormalClosure, "")
	w.cancel()
}

func (w *Websocket) notifyError(err error) {
	w.mu.Lock()
	h := w.handler
	w.mu.Unlock()
	if h != nil {
		h.OnError(err)
	}
}
