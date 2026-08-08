package transport

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPServing is implemented by transports that accept plain HTTP requests
// (the HTTP long-polling transport).
type HTTPServing interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

const pollingCloseTimeout = 30 * time.Second

// Polling is the HTTP long-polling transport.
//
// It follows the semantics of the reference implementation:
//   - a single pending GET request receives buffered packets (long-polling);
//   - a single POST request delivers a payload from the client;
//   - the transport is "writable" only while a GET request is pending;
//   - an orderly close is deferred until the next poll so a `close` packet
//     can be delivered to the client.
type Polling struct {
	mu sync.Mutex

	handler Handler
	logger  Logger

	req      *http.Request       // pending GET poll
	res      http.ResponseWriter // pending GET response
	pollDone chan struct{}       // closed once the pending poll is fulfilled
	dataReq  *http.Request       // active POST request

	writable     bool
	discarded    bool
	closed       bool
	pendingClose bool
	closeTimer   *time.Timer

	maxBufferSize int64
}

// NewPolling creates a polling transport. The default maximum accepted
// request body size is 1 MiB.
func NewPolling() *Polling {
	return &Polling{maxBufferSize: 1 << 20}
}

// SetMaxBufferSize sets the maximum accepted POST body size. Requests larger
// than this are rejected with HTTP 413.
func (p *Polling) SetMaxBufferSize(n int64) {
	p.mu.Lock()
	p.maxBufferSize = n
	p.mu.Unlock()
}

// SetLogger sets the logger used by the transport.
func (p *Polling) SetLogger(l Logger) {
	p.mu.Lock()
	p.logger = l
	p.mu.Unlock()
}

func (p *Polling) Name() string { return "polling" }

func (p *Polling) Writable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writable
}

func (p *Polling) SetWritable(v bool) {
	p.mu.Lock()
	p.writable = v
	p.mu.Unlock()
}

func (p *Polling) SetHandler(h Handler) {
	p.mu.Lock()
	p.handler = h
	p.mu.Unlock()
}

// ServeHTTP handles a single polling request.
func (p *Polling) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.onPollRequest(w, r)
	case http.MethodPost:
		p.onDataRequest(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusInternalServerError)
	}
}

// onPollRequest blocks until a response is written by Send (or the request is
// aborted / the transport is closed). The single writer guarantee of
// net/http holds because the response is only ever written from Send.
func (p *Polling) onPollRequest(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if p.req != nil {
		p.mu.Unlock()
		p.notifyError(ErrRequestOverlap)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	p.req = r
	p.res = w
	p.writable = true
	done := make(chan struct{})
	p.pollDone = done
	h := p.handler
	p.mu.Unlock()

	// Detect a prematurely closed poll connection and unblock on close.
	go func() {
		select {
		case <-r.Context().Done():
			p.mu.Lock()
			if p.req == r {
				p.req = nil
				p.res = nil
				p.pollDone = nil
				p.mu.Unlock()
				close(done)
				if h != nil {
					h.OnError(ErrPollClosed)
				}
				return
			}
			p.mu.Unlock()
		case <-done:
		}
	}()

	if h != nil {
		h.OnReady()
	}

	p.mu.Lock()
	pending := p.pendingClose && !p.closed
	p.mu.Unlock()
	if pending {
		// An orderly close was requested while the transport was not
		// writable; deliver it now that a poll is available.
		p.Send([]*Packet{{Type: Noop}})
	}

	<-done
}

func (p *Polling) onDataRequest(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if p.dataReq != nil {
		p.mu.Unlock()
		p.notifyError(ErrDataRequestActive)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	p.dataReq = r
	maxBufferSize := p.maxBufferSize
	h := p.handler
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.dataReq = nil
		p.mu.Unlock()
	}()

	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/octet-stream") {
		p.notifyError(errors.New("engineio: invalid content type for polling payload"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBufferSize+1))
	if err != nil {
		p.notifyError(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if int64(len(body)) > maxBufferSize {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}

	pkts, err := DecodePayload(body)
	if err != nil {
		if h != nil {
			h.OnError(err)
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for _, pk := range pkts {
		if h != nil {
			h.OnPacket(pk)
		}
	}

	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// Send delivers a batch of packets through the currently pending poll
// request, if any. The transport takes ownership of pkts.
func (p *Polling) Send(pkts []*Packet) {
	var pendingClose bool
	var h Handler
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	pendingClose = p.pendingClose
	if pendingClose {
		pkts = append(pkts, &Packet{Type: Close})
	}
	p.writable = false
	res := p.res
	done := p.pollDone
	if res == nil {
		// No poll request pending; nothing can be delivered right now.
		p.mu.Unlock()
		return
	}
	p.req = nil
	p.res = nil
	p.pollDone = nil
	h = p.handler
	encoded := EncodePayload(pkts)
	p.mu.Unlock()

	res.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	res.Header().Set("Cache-Control", "no-store")
	res.WriteHeader(http.StatusOK)
	_, _ = res.Write(encoded)
	if f, ok := res.(http.Flusher); ok {
		f.Flush()
	}

	if done != nil {
		close(done)
	}

	if pendingClose {
		p.notifyClose()
	}
	if h != nil {
		h.OnDrain()
	}
}

// Discard marks the transport as discarded, so Close will not attempt to
// deliver a close packet.
func (p *Polling) Discard() {
	p.mu.Lock()
	p.discarded = true
	p.mu.Unlock()
}

// Close initiates an orderly close. If a poll request is pending, a `close`
// packet is delivered immediately; otherwise the close is deferred until the
// next poll or a 30s timeout. Discarded transports are closed without
// delivering any further packet.
func (p *Polling) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.discarded {
		p.mu.Unlock()
		p.notifyClose()
		return
	}
	if p.writable {
		p.mu.Unlock()
		p.Send([]*Packet{{Type: Close}})
		p.notifyClose()
		return
	}
	p.pendingClose = true
	if p.closeTimer == nil {
		p.closeTimer = time.AfterFunc(pollingCloseTimeout, func() {
			p.notifyClose()
		})
	}
	p.mu.Unlock()
}

// closeLocked transitions the transport to closed and returns the handler to
// notify. The caller must not hold the mutex when calling handler methods.
func (p *Polling) closeLocked() Handler {
	if p.closed {
		return nil
	}
	p.closed = true
	p.writable = false
	p.pendingClose = false
	p.req = nil
	p.res = nil
	p.dataReq = nil
	if p.closeTimer != nil {
		p.closeTimer.Stop()
		p.closeTimer = nil
	}
	if p.pollDone != nil {
		close(p.pollDone)
		p.pollDone = nil
	}
	return p.handler
}

func (p *Polling) notifyClose() {
	p.mu.Lock()
	h := p.closeLocked()
	p.mu.Unlock()
	if h != nil {
		h.OnClose()
	}
}

func (p *Polling) notifyError(err error) {
	p.mu.Lock()
	h := p.handler
	p.mu.Unlock()
	if h != nil {
		h.OnError(err)
	}
}
