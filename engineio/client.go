package engineio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/kingecg/gosocketio/engineio/transport"
)

// Client is an Engine.IO client. It establishes a session with an Engine.IO
// server over HTTP long-polling and/or WebSocket, keeps the heartbeat alive
// and delivers message packets.
type Client struct {
	u      *url.URL
	opts   Options
	logger Logger

	mu       sync.Mutex
	sid      string
	tport    transport.Transport
	closed   bool
	closeErr error

	// writeBuffer holds packets that cannot be flushed yet (e.g. while a
	// transport upgrade is in progress). upgrading disables direct flushing so
	// sends during the upgrade are delivered over the post-upgrade transport
	// instead of being dropped by the discarded polling transport.
	writeBuffer []*transport.Packet
	upgrading   bool

	pingInterval       time.Duration
	pingTimeout        time.Duration
	noPingTimeoutTimer *time.Timer

	handshakeDone chan error
	handshakeOnce sync.Once

	onOpen  func(*Client)
	onData  func(*Client, []byte, bool)
	onClose func(*Client, error)
}

// Dial connects to an Engine.IO server. It blocks until the handshake
// completes or ctx is cancelled.
func Dial(ctx context.Context, rawURL string, opts *Options) (*Client, error) {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o = o.withDefaults()

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	c := &Client{
		u:             u,
		opts:          o,
		logger:        defaultLogger,
		handshakeDone: make(chan error, 1),
	}
	c.onOpen = func(*Client) {}
	c.onData = func(*Client, []byte, bool) {}
	c.onClose = func(*Client, error) {}

	first := o.Transports[0]
	if first == "polling" {
		c.dialPolling(ctx)
	} else if first == "websocket" {
		err := c.dialWebsocket(ctx)
		if err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		c.Close()
		return nil, ctx.Err()
	case err := <-c.handshakeDone:
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

// SetLogger sets the client logger.
func (c *Client) SetLogger(l Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l != nil {
		c.logger = l
	}
}

// OnOpen registers a handler invoked once the handshake completes.
func (c *Client) OnOpen(f func(*Client)) {
	if f != nil {
		c.onOpen = f
	}
}

// OnData registers a handler invoked for every received message packet.
func (c *Client) OnData(f func(*Client, []byte, bool)) {
	if f != nil {
		c.onData = f
	}
}

// OnClose registers a handler invoked when the session is closed.
func (c *Client) OnClose(f func(*Client, error)) {
	if f != nil {
		c.onClose = f
	}
}

// SID returns the session identifier once the handshake has completed.
func (c *Client) SID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sid
}

// SendMessage sends a message packet to the server.
func (c *Client) SendMessage(data []byte, binary bool) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("engineio: client is closed")
	}
	c.writeBuffer = append(c.writeBuffer, &transport.Packet{Type: transport.Message, Data: data, IsBinary: binary})
	c.mu.Unlock()
	c.flushSends()
	return nil
}

// Close closes the session.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	if c.noPingTimeoutTimer != nil {
		c.noPingTimeoutTimer.Stop()
	}
	tport := c.tport
	c.mu.Unlock()
	if tport != nil {
		tport.Close()
	}
	return nil
}

func (c *Client) dialPolling(ctx context.Context) {
	u := *c.u
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "polling")
	u.RawQuery = q.Encode()

	p := &clientPolling{
		httpClient: &http.Client{},
		u:          &u,
		sid:        c.SID,
		pauseCh:    make(chan struct{}),
	}
	p.inflightCond = sync.NewCond(&p.mu)
	// The transport outlives the Dial context: its lifecycle is tied to the
	// session (Close), not the initial connect attempt.
	p.ctx, p.cancel = context.WithCancel(context.Background())
	c.mu.Lock()
	c.tport = p
	c.mu.Unlock()
	p.SetHandler(&clientBinding{client: c, transport: p})
}

func (c *Client) dialWebsocket(ctx context.Context) error {
	u := *c.u
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()
	wsURL := wsURLString(&u)

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	wt := &clientWebsocket{conn: conn}
	// The transport outlives the Dial context: its lifecycle is tied to the
	// session (Close), not the initial connect attempt.
	wt.ctx, wt.cancel = context.WithCancel(context.Background())
	c.mu.Lock()
	c.tport = wt
	c.mu.Unlock()
	wt.SetHandler(&clientBinding{client: c, transport: wt})
	return nil
}

func (c *Client) handshakeSucceeded() {
	c.handshakeOnce.Do(func() {
		c.handshakeDone <- nil
	})
}

func (c *Client) handshakeFailed(err error) {
	c.handshakeOnce.Do(func() {
		c.handshakeDone <- err
	})
}

func (c *Client) onPacket(p *transport.Packet) {
	switch p.Type {
	case transport.Open:
		c.handleOpen(p.Data)
	case transport.Ping:
		c.sendPacket(transport.Pong, p.Data, false)
		c.resetNoPingTimeout()
	case transport.Message:
		c.mu.Lock()
		onData := c.onData
		c.mu.Unlock()
		if onData != nil {
			onData(c, p.Data, p.IsBinary)
		}
	case transport.Close:
		c.closeWithError(nil)
	}
}

func (c *Client) handleOpen(data []byte) {
	var o struct {
		SID          string   `json:"sid"`
		Upgrades     []string `json:"upgrades"`
		PingInterval int64    `json:"pingInterval"`
		PingTimeout  int64    `json:"pingTimeout"`
		MaxPayload   int64    `json:"maxPayload"`
	}
	if err := json.Unmarshal(data, &o); err != nil {
		c.closeWithError(err)
		return
	}
	c.mu.Lock()
	c.sid = o.SID
	c.pingInterval = time.Duration(o.PingInterval) * time.Millisecond
	c.pingTimeout = time.Duration(o.PingTimeout) * time.Millisecond
	upgrades := o.Upgrades
	c.mu.Unlock()

	c.resetNoPingTimeout()

	if contains(upgrades, "websocket") && c.opts.AllowUpgrades && c.opts.transportsEnabled()["websocket"] {
		go c.upgradeToWebsocket()
	}

	c.mu.Lock()
	onOpen := c.onOpen
	c.mu.Unlock()
	c.handshakeSucceeded()
	if onOpen != nil {
		onOpen(c)
	}
}

// upgradeToWebsocket performs the transport upgrade handshake
// (ping probe -> pong probe -> upgrade packet) and switches the active
// transport on success. A failed upgrade leaves polling in place.
func (c *Client) upgradeToWebsocket() {
	c.mu.Lock()
	sid := c.sid
	tport := c.tport
	logger := c.logger
	c.mu.Unlock()
	if sid == "" {
		return
	}
	// Buffer all sends while the upgrade handshake is in flight so no packet
	// is sent over the outgoing polling transport once the server switches.
	c.mu.Lock()
	c.upgrading = true
	c.mu.Unlock()
	// Pause the polling transport before starting the upgrade handshake.
	if p, ok := tport.(*clientPolling); ok {
		p.pause()
	}

	u := *c.u
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	q.Set("sid", sid)
	u.RawQuery = q.Encode()
	wsURL := wsURLString(&u)

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.UpgradeTimeout)
	defer cancel()

	// On any failure, drop the upgrade state, flush buffered packets over the
	// current transport, and resume polling so the session continues.
	fail := func() {
		c.mu.Lock()
		c.upgrading = false
		c.mu.Unlock()
		c.flushSends()
		if p, ok := tport.(*clientPolling); ok {
			p.resume()
		}
	}

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		logger.Debugf("engineio: upgrade dial failed: %v", err)
		fail()
		return
	}

	if err := conn.Write(ctx, websocket.MessageText, []byte("2probe")); err != nil {
		logger.Debugf("engineio: upgrade probe write failed: %v", err)
		_ = conn.CloseNow()
		fail()
		return
	}

	probeOK := false
	for !probeOK {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			logger.Debugf("engineio: upgrade probe read failed: %v", err)
			_ = conn.CloseNow()
			fail()
			return
		}
		if typ == websocket.MessageText && string(data) == "3probe" {
			probeOK = true
		} else {
			logger.Debugf("engineio: upgrade probe got unexpected %q", data)
		}
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte("5")); err != nil {
		logger.Debugf("engineio: upgrade write failed: %v", err)
		_ = conn.CloseNow()
		fail()
		return
	}

	wt := &clientWebsocket{conn: conn}
	wt.ctx, wt.cancel = context.WithCancel(context.Background())

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.CloseNow()
		return
	}
	old := c.tport
	c.tport = wt
	c.upgrading = false
	c.mu.Unlock()

	if p, ok := old.(*clientPolling); ok {
		p.stopForUpgrade()
	} else {
		old.Close()
	}
	wt.SetHandler(&clientBinding{client: c, transport: wt})
	c.flushSends()
	logger.Debugf("engineio[%s]: upgraded to websocket", c.sid)
}

func (c *Client) sendPacket(t transport.Type, data []byte, binary bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.writeBuffer = append(c.writeBuffer, &transport.Packet{Type: t, Data: data, IsBinary: binary})
	c.mu.Unlock()
	c.flushSends()
}

// flushSends delivers any buffered packets to the active transport. While a
// transport upgrade is in progress, or when no transport is active yet, the
// buffer is left untouched.
func (c *Client) flushSends() {
	c.mu.Lock()
	if c.closed || c.upgrading {
		c.mu.Unlock()
		return
	}
	tport := c.tport
	buf := c.writeBuffer
	c.writeBuffer = nil
	c.mu.Unlock()
	if tport != nil && len(buf) > 0 {
		tport.Send(buf)
	}
}

// resetNoPingTimeout restarts the client-side heartbeat watchdog: if the
// server does not send a ping within pingInterval+pingTimeout, the session
// is considered dead.
func (c *Client) resetNoPingTimeout() {
	interval := c.pingInterval
	timeout := c.pingTimeout
	if interval == 0 {
		interval = c.opts.PingInterval
	}
	if timeout == 0 {
		timeout = c.opts.PingTimeout
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.noPingTimeoutTimer != nil {
		c.noPingTimeoutTimer.Stop()
	}
	c.noPingTimeoutTimer = time.AfterFunc(interval+timeout, func() {
		c.closeWithError(fmt.Errorf("%w: ping timeout", ErrHeartbeatTimeout))
	})
	c.mu.Unlock()
}

func (c *Client) closeWithError(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.noPingTimeoutTimer != nil {
		c.noPingTimeoutTimer.Stop()
	}
	onClose := c.onClose
	handshakeDone := c.handshakeDone
	c.mu.Unlock()
	if err != nil {
		c.handshakeFailed(err)
	}
	if onClose != nil {
		onClose(c, err)
	}
	_ = handshakeDone
}

func (c *Client) onErrorFrom(t transport.Transport, err error) {
	c.mu.Lock()
	active := c.tport == t
	logger := c.logger
	c.mu.Unlock()
	if !active {
		return
	}
	logger.Debugf("engineio: transport error: %v", err)
	c.closeWithError(wrapInvalidPacket(err))
}

func (c *Client) onCloseFrom(t transport.Transport) {
	c.mu.Lock()
	active := c.tport == t
	c.mu.Unlock()
	if !active {
		return
	}
	c.closeWithError(errors.New("engineio: transport closed"))
}

// clientBinding dispatches transport events to the client, remembering which
// transport they came from so that events from a replaced transport are
// ignored.
type clientBinding struct {
	client    *Client
	transport transport.Transport
}

func (b *clientBinding) OnPacket(p *transport.Packet) {
	b.client.mu.Lock()
	active := b.client.tport == b.transport
	b.client.mu.Unlock()
	if !active && p.Type == transport.Close {
		// Close notifications from a replaced transport (e.g. the discarded
		// server-side polling transport flushing its pending poll) must not
		// terminate the session.
		return
	}
	b.client.onPacket(p)
}
func (b *clientBinding) OnError(err error) { b.client.onErrorFrom(b.transport, err) }
func (b *clientBinding) OnClose()          { b.client.onCloseFrom(b.transport) }
func (b *clientBinding) OnDrain()          {}
func (b *clientBinding) OnReady()          {}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// wsURLString converts an http(s) URL into a ws(s) URL string.
func wsURLString(u *url.URL) string {
	n := *u
	switch n.Scheme {
	case "http":
		n.Scheme = "ws"
	case "https":
		n.Scheme = "wss"
	}
	return n.String()
}
