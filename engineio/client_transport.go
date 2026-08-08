package engineio

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/coder/websocket"
	"github.com/kingecg/gosocketio/engineio/transport"
)

// clientPolling is the client-side HTTP long-polling transport.
type clientPolling struct {
	httpClient *http.Client
	u          *url.URL
	sid        func() string

	handler transport.Handler

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	closed   bool
	paused   bool
	pollOnce sync.Once
}

func (p *clientPolling) Name() string { return "polling" }

func (p *clientPolling) Writable() bool { return true }

func (p *clientPolling) SetWritable(v bool) {}

func (p *clientPolling) SetHandler(h transport.Handler) {
	p.handler = h
	p.pollOnce.Do(func() { go p.pollLoop() })
}

// pause stops the polling loop once the current poll request returns. It is
// used during a transport upgrade so that no further GET requests are issued
// over the outgoing transport.
func (p *clientPolling) pause() {
	p.mu.Lock()
	p.paused = true
	p.mu.Unlock()
}

// stopForUpgrade marks the transport as closed without cancelling in-flight
// requests, so pending sends complete normally during the upgrade switchover
// instead of being silently aborted.
func (p *clientPolling) stopForUpgrade() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// resume re-enables the polling loop after a failed upgrade attempt.
func (p *clientPolling) resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

func (p *clientPolling) Send(pkts []*transport.Packet) {
	body := transport.EncodePayload(pkts)
	u := p.urlWithSID()
	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		p.handler.OnError(err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		if p.ctx.Err() != nil {
			return // closed
		}
		p.handler.OnError(err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		p.handler.OnError(&httpError{code: resp.StatusCode})
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

func (p *clientPolling) pollLoop() {
	for {
		p.mu.Lock()
		closed := p.closed
		paused := p.paused
		p.mu.Unlock()
		if closed || paused {
			return
		}
		if p.ctx.Err() != nil {
			return
		}

		u := p.urlWithSID()
		req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			p.handler.OnError(err)
			return
		}
		resp, err := p.httpClient.Do(req)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			p.handler.OnError(err)
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			p.handler.OnError(&httpError{code: resp.StatusCode})
			return
		}
		if readErr != nil {
			p.handler.OnError(readErr)
			return
		}

		pkts, err := transport.DecodePayload(body)
		if err != nil {
			p.handler.OnError(err)
			return
		}
		for _, pk := range pkts {
			p.handler.OnPacket(pk)
		}
	}
}

func (p *clientPolling) urlWithSID() url.URL {
	u := *p.u
	q := u.Query()
	if sid := p.sid(); sid != "" {
		q.Set("sid", sid)
	}
	u.RawQuery = q.Encode()
	return u
}

func (p *clientPolling) Discard() {}

func (p *clientPolling) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	p.cancel()
}

type httpError struct {
	code int
}

func (e *httpError) Error() string {
	return "engineio: unexpected http status " + http.StatusText(e.code)
}

// clientWebsocket is the client-side WebSocket transport.
type clientWebsocket struct {
	conn *websocket.Conn

	ctx    context.Context
	cancel context.CancelFunc

	handler  transport.Handler
	readOnce sync.Once
}

func (w *clientWebsocket) Name() string { return "websocket" }

func (w *clientWebsocket) Writable() bool { return true }

func (w *clientWebsocket) SetWritable(v bool) {}

func (w *clientWebsocket) SetHandler(h transport.Handler) {
	w.handler = h
	w.readOnce.Do(func() { go w.readLoop() })
}

func (w *clientWebsocket) readLoop() {
	for {
		typ, data, err := w.conn.Read(w.ctx)
		if err != nil {
			if w.handler != nil {
				w.handler.OnClose()
			}
			return
		}
		var p transport.Packet
		if typ == websocket.MessageBinary {
			p = transport.Packet{Type: transport.Message, Data: data, IsBinary: true}
		} else {
			dec, err := transport.DecodePacket(data, false)
			if err != nil {
				if w.handler != nil {
					w.handler.OnError(err)
				}
				continue
			}
			p = dec
		}
		if w.handler != nil {
			w.handler.OnPacket(&p)
		}
	}
}

func (w *clientWebsocket) Send(pkts []*transport.Packet) {
	for _, p := range pkts {
		if p.IsBinary {
			if err := w.conn.Write(w.ctx, websocket.MessageBinary, p.Data); err != nil {
				w.notifyError(err)
				return
			}
		} else {
			if err := w.conn.Write(w.ctx, websocket.MessageText, transport.EncodePacket(p, true)); err != nil {
				w.notifyError(err)
				return
			}
		}
	}
}

func (w *clientWebsocket) Discard() {}

func (w *clientWebsocket) Close() {
	_ = w.conn.Close(websocket.StatusNormalClosure, "")
	w.cancel()
}

func (w *clientWebsocket) notifyError(err error) {
	if w.handler != nil {
		w.handler.OnError(err)
	}
}
