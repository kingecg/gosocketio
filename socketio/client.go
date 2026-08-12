package socketio

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/kingecg/gosocketio/engineio"
)

// Options configures a Socket.IO client session.
type Options struct {
	// Transports overrides the Engine.IO transports.
	Transports []string

	// Engine passes extra options to engineio.Dial.
	Engine *engineio.Options

	// Auth is sent as the root namespace CONNECT payload.
	Auth map[string]any

	// Reconnection enables automatic reconnection after an unexpected
	// close of the underlying transport.
	Reconnection bool

	// ReconnectionAttempts limits consecutive reconnection attempts. Zero
	// (the default) means retry forever.
	ReconnectionAttempts int

	// ReconnectionDelay is the base delay before the first reconnection
	// attempt. Default: 1s.
	ReconnectionDelay time.Duration

	// ReconnectionDelayMax caps the exponentially backed-off delay.
	// Default: 5s.
	ReconnectionDelayMax time.Duration

	// RandomizationFactor in [0,1] jitters the delay so simultaneous
	// clients do not reconnect in lockstep. Default: 0.5.
	RandomizationFactor float64

	// Timeout bounds each reconnection attempt (Engine.IO dial plus all
	// namespace CONNECTs). Default: 10s.
	Timeout time.Duration
}

func (o *Options) withDefaults() Options {
	opts := *o
	if opts.ReconnectionDelay == 0 {
		opts.ReconnectionDelay = time.Second
	}
	if opts.ReconnectionDelayMax == 0 {
		opts.ReconnectionDelayMax = 5 * time.Second
	}
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Second
	}
	return opts
}

// Client is a Socket.IO client. It maintains one Engine.IO connection
// multiplexing any number of namespaces, and mirrors the official
// socket.io-client behaviour for events, acknowledgements, binary payloads and
// reconnection.
type Client struct {
	rawURL string
	opts   Options
	logger engineio.Logger

	mu     sync.Mutex
	engine *engineio.Client
	nsps   map[string]*nsClient
	acks   *ackTable
	closed bool

	// handlers registered per namespace
	onConnect      map[string][]reflect.Value
	onEvent        map[string]map[string][]reflect.Value
	onDisconnect   map[string][]reflect.Value
	onConnectError map[string][]reflect.Value

	// connecting holds the ack channel for a CONNECT packet still awaited.
	// It is populated by connectNsp before the CONNECT packet is sent and
	// removed when the response (or a timeout) arrives.
	connecting map[string]chan error

	// inbound packets are serialized through one ordered queue so binary
	// reconstruction and handler dispatch stay ordered.
	in      *packetQueue
	recon   *reconstructor
	started sync.Once

	reconnectStop     chan struct{}
	reconnectStopOnce sync.Once
}

// nsClient tracks the connection state of one namespace.
type nsClient struct {
	nsp       string
	connected bool
	sid       string
}

// Dial connects to a Socket.IO server. It blocks until the root namespace
// CONNECT completes or ctx is cancelled.
func Dial(ctx context.Context, rawURL string, opts *Options) (*Client, error) {
	c := newClient(rawURL, opts)
	if err := c.dialEngine(ctx); err != nil {
		return nil, err
	}
	if err := c.connectNsp(ctx, defaultNamespace, c.opts.Auth); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// SetLogger sets the client logger.
func (c *Client) SetLogger(l engineio.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l != nil {
		c.logger = l
	}
}

func (c *Client) logf() engineio.Logger {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logger
}

// OnEvent registers a handler invoked when an event is received on a
// namespace. Handler return values form the acknowledgement payload when the
// server requested one.
func (c *Client) OnEvent(nsp, event string, f any) {
	v := reflect.ValueOf(f)
	if v.Kind() != reflect.Func {
		panic("socketio: event handler must be a function")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.onEvent[nsp] == nil {
		c.onEvent[nsp] = make(map[string][]reflect.Value)
	}
	c.onEvent[nsp][event] = append(c.onEvent[nsp][event], v)
}

// OnConnect registers a handler invoked when a namespace connection completes.
// It fires again after each successful reconnection.
func (c *Client) OnConnect(nsp string, f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnect[nsp] = append(c.onConnect[nsp], reflect.ValueOf(f))
}

// OnDisconnect registers a handler invoked when a namespace disconnects,
// carrying the Socket.IO disconnect reason.
func (c *Client) OnDisconnect(nsp string, f func(reason string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect[nsp] = append(c.onDisconnect[nsp], reflect.ValueOf(f))
}

// OnConnectError registers a handler invoked when a namespace connection is
// rejected, carrying the error reported by the server.
func (c *Client) OnConnectError(nsp string, f func(err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnectError[nsp] = append(c.onConnectError[nsp], reflect.ValueOf(f))
}

// Connected reports whether the namespace is currently connected.
func (c *Client) Connected(nsp string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	nc := c.nsps[nsp]
	return nc != nil && nc.connected
}

// ID returns the namespace-scoped socket id assigned by the server.
func (c *Client) ID(nsp string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nc := c.nsps[nsp]; nc != nil {
		return nc.sid
	}
	return ""
}

// ConnectNamespace connects an additional namespace, blocking until the
// CONNECT acknowledgement arrives or ctx is cancelled.
func (c *Client) ConnectNamespace(ctx context.Context, nsp string, data map[string]any) error {
	if nsp == "" || nsp == defaultNamespace {
		return errors.New("socketio: use Dial to connect the default namespace")
	}
	return c.connectNsp(ctx, nsp, data)
}

// DisconnectNamespace disconnects a namespace, notifying the server.
func (c *Client) DisconnectNamespace(nsp string) {
	c.mu.Lock()
	nc := c.nsps[nsp]
	fire := nc != nil && nc.connected
	if fire {
		nc.connected = false
	}
	handlers := c.onDisconnect[nsp]
	c.mu.Unlock()
	c.sendPacket(&Packet{Type: Disconnect, Nsp: nsp, ID: -1})
	if fire {
		for _, h := range handlers {
			go h.Call([]reflect.Value{reflect.ValueOf("io client disconnect")})
		}
	}
}

// Emit sends an event on a namespace without an acknowledgement.
func (c *Client) Emit(nsp, event string, args ...any) error {
	if !c.Connected(nsp) {
		return ErrNotConnected
	}
	c.sendPacket(&Packet{Type: Event, Nsp: nsp, ID: -1, Data: append([]any{event}, args...)})
	return nil
}

// EmitWithAck sends an event that requires an acknowledgement. cb is invoked
// with the server's reply once it arrives.
func (c *Client) EmitWithAck(nsp, event string, cb func(args []any), args ...any) (int64, error) {
	if !c.Connected(nsp) {
		return -1, ErrNotConnected
	}
	if cb == nil {
		c.sendPacket(&Packet{Type: Event, Nsp: nsp, ID: -1, Data: append([]any{event}, args...)})
		return -1, nil
	}
	id := c.acks.next()
	c.acks.register(id, cb)
	c.sendPacket(&Packet{Type: Event, Nsp: nsp, ID: id, Data: append([]any{event}, args...)})
	return id, nil
}

// Close terminates the session. Pending acknowledgements are dropped and every
// connected namespace reports a disconnect with reason "io client disconnect".
func (c *Client) Close() error {
	c.reconnectStopOnce.Do(func() { close(c.reconnectStop) })
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	engine := c.engine
	c.engine = nil
	var nsps []*nsClient
	for _, nc := range c.nsps {
		if nc.connected {
			nc.connected = false
			nsps = append(nsps, nc)
		}
	}
	c.mu.Unlock()
	c.acks.clear()
	for _, nc := range nsps {
		c.fireDisconnect(nc.nsp, "io client disconnect")
	}
	if engine != nil {
		return engine.Close()
	}
	return nil
}

func (c *Client) newEngineOptions() *engineio.Options {
	var eo *engineio.Options
	if c.opts.Engine != nil {
		eo = &engineio.Options{}
		*eo = *c.opts.Engine
	}
	if len(c.opts.Transports) > 0 {
		if eo == nil {
			eo = &engineio.Options{}
		}
		eo.Transports = c.opts.Transports
	}
	return eo
}

func (c *Client) dialEngine(ctx context.Context) error {
	eng, err := engineio.Dial(ctx, c.rawURL, c.newEngineOptions())
	if err != nil {
		return err
	}
	eng.SetLogger(c.logger)
	eng.OnData(func(_ *engineio.Client, data []byte, binary bool) {
		c.in.push(&inPacket{data: data, binary: binary})
	})
	eng.OnClose(func(_ *engineio.Client, err error) {
		c.onEngineClosed(err)
	})
	c.mu.Lock()
	c.engine = eng
	c.mu.Unlock()
	c.start()
	return nil
}

func (c *Client) start() {
	c.started.Do(func() { go c.processLoop() })
}

func (c *Client) processLoop() {
	for {
		p, ok := c.in.pop()
		if !ok {
			return
		}
		c.handleIn(p.data, p.binary)
	}
}

// connectNsp sends a CONNECT packet for the namespace and waits for the
// acknowledgement. The namespace is marked connected only once the Connect
// packet actually arrives, so a late reply after a timeout is ignored.
func (c *Client) connectNsp(ctx context.Context, nsp string, data map[string]any) error {
	ch := make(chan error, 1)
	c.mu.Lock()
	nc := c.nsps[nsp]
	if nc == nil {
		nc = &nsClient{nsp: nsp}
		c.nsps[nsp] = nc
	}
	nc.connected = false
	c.connecting[nsp] = ch
	c.mu.Unlock()

	var payload any
	if data != nil {
		payload = data
	}
	c.sendPacket(&Packet{Type: Connect, Nsp: nsp, ID: -1, Data: payload})

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.connecting, nsp)
		c.mu.Unlock()
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (c *Client) handleIn(data []byte, binary bool) {
	if binary {
		c.feedBinary(data)
		return
	}
	pkt, err := Decode(data)
	if err != nil {
		c.logf().Debugf("socketio: decode error: %v", err)
		return
	}
	if pkt.Type == BinaryEvent || pkt.Type == BinaryAck {
		if pkt.Attachments == 0 {
			pkt.Type = binaryToPlain(pkt.Type)
			c.process(pkt)
			return
		}
		c.recon = &reconstructor{pkt: pkt}
		return
	}
	c.process(pkt)
}

func (c *Client) feedBinary(data []byte) {
	rec := c.recon
	if rec == nil {
		c.logf().Debugf("socketio: unexpected binary data")
		return
	}
	rec.bufs = append(rec.bufs, data)
	if len(rec.bufs) != rec.pkt.Attachments {
		return
	}
	c.recon = nil
	pkt := *rec.pkt
	pkt.Data = reconstruct(pkt.Data, rec.bufs)
	pkt.Type = binaryToPlain(pkt.Type)
	c.process(&pkt)
}

func (c *Client) process(pkt *Packet) {
	switch pkt.Type {
	case Connect:
		c.handleConnect(pkt)
	case ConnectError:
		c.handleConnectError(pkt)
	case Disconnect:
		c.handleDisconnect(pkt)
	case Event:
		c.handleEvent(pkt)
	case Ack:
		c.handleAck(pkt)
	default:
		c.logf().Debugf("socketio: ignoring packet %v", pkt.Type)
	}
}

func (c *Client) handleConnect(pkt *Packet) {
	nsp := pkt.Nsp
	c.mu.Lock()
	ch := c.connecting[nsp]
	if ch != nil {
		delete(c.connecting, nsp)
	}
	nc := c.nsps[nsp]
	if nc != nil && ch != nil {
		nc.connected = true
		if m, ok := pkt.Data.(map[string]any); ok {
			if s, ok := m["sid"].(string); ok {
				nc.sid = s
			}
		}
	}
	handlers := c.onConnect[nsp]
	c.mu.Unlock()
	if ch != nil {
		ch <- nil
	}
	for _, h := range handlers {
		go h.Call(nil)
	}
}

func (c *Client) handleConnectError(pkt *Packet) {
	nsp := pkt.Nsp
	msg := ""
	if m, ok := pkt.Data.(map[string]any); ok {
		if s, ok := m["message"].(string); ok {
			msg = s
		}
	}
	c.mu.Lock()
	ch := c.connecting[nsp]
	if ch != nil {
		delete(c.connecting, nsp)
	}
	if nc := c.nsps[nsp]; nc != nil {
		nc.connected = false
	}
	handlers := c.onConnectError[nsp]
	c.mu.Unlock()
	var err error
	if msg != "" {
		err = errors.New(msg)
	} else {
		err = errors.New("socketio: connection rejected")
	}
	if ch != nil {
		ch <- err
	}
	for _, h := range handlers {
		go h.Call([]reflect.Value{reflect.ValueOf(err)})
	}
}

func (c *Client) handleDisconnect(pkt *Packet) {
	nsp := pkt.Nsp
	c.mu.Lock()
	nc := c.nsps[nsp]
	fire := nc != nil && nc.connected
	if fire {
		nc.connected = false
	}
	handlers := c.onDisconnect[nsp]
	c.mu.Unlock()
	if fire {
		for _, h := range handlers {
			go h.Call([]reflect.Value{reflect.ValueOf("io server disconnect")})
		}
	}
}

func (c *Client) handleEvent(pkt *Packet) {
	data, ok := pkt.Data.([]any)
	if !ok || len(data) == 0 {
		return
	}
	name, ok := data[0].(string)
	if !ok {
		return
	}
	args := data[1:]
	nsp := pkt.Nsp

	c.mu.Lock()
	handlers := c.onEvent[nsp][name]
	c.mu.Unlock()

	// Handlers run on their own goroutines so a handler that blocks (for
	// example waiting for a round-trip acknowledgement) does not stall the
	// ordered packet processing for this connection.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []any
	for _, h := range handlers {
		wg.Add(1)
		go func(h reflect.Value) {
			defer wg.Done()
			res := invokeClientHandler(h, args)
			mu.Lock()
			results = append(results, res...)
			mu.Unlock()
		}(h)
	}
	if pkt.ID >= 0 {
		go func() {
			wg.Wait()
			if results == nil {
				results = []any{}
			}
			c.sendPacket(&Packet{Type: Ack, Nsp: nsp, ID: pkt.ID, Data: results})
		}()
	}
}

func (c *Client) handleAck(pkt *Packet) {
	data, _ := pkt.Data.([]any)
	if data == nil {
		data = []any{}
	}
	go c.acks.invoke(pkt.ID, data)
}

// sendPacket encodes and writes a packet to the underlying Engine.IO
// connection, splitting binary payloads into a placeholder text packet
// followed by the raw binary buffers.
func (c *Client) sendPacket(p *Packet) {
	c.mu.Lock()
	engine := c.engine
	c.mu.Unlock()
	if engine == nil {
		return
	}
	if p.Data != nil && hasBinary(p.Data) {
		var bufs [][]byte
		p.Data = deconstruct(p.Data, &bufs)
		switch p.Type {
		case Event:
			p.Type = BinaryEvent
		case Ack:
			p.Type = BinaryAck
		}
		p.Attachments = len(bufs)
		engine.SendMessage(p.Encode(), false)
		for _, b := range bufs {
			engine.SendMessage(b, true)
		}
		return
	}
	engine.SendMessage(p.Encode(), false)
}

func (c *Client) fireConnectError(nsp string, err error) {
	c.mu.Lock()
	handlers := c.onConnectError[nsp]
	c.mu.Unlock()
	for _, h := range handlers {
		go h.Call([]reflect.Value{reflect.ValueOf(err)})
	}
}

func (c *Client) fireDisconnect(nsp, reason string) {
	c.mu.Lock()
	handlers := c.onDisconnect[nsp]
	c.mu.Unlock()
	for _, h := range handlers {
		go h.Call([]reflect.Value{reflect.ValueOf(reason)})
	}
}

// onEngineClosed is invoked by the Engine.IO layer when the session ends
// unexpectedly. Namespace state is cleaned up and, when reconnection is
// enabled, a reconnection attempt is scheduled.
func (c *Client) onEngineClosed(err error) {
	reason := mapClientDisconnectReason(err)
	c.mu.Lock()
	for nsp, ch := range c.connecting {
		select {
		case ch <- err:
		default:
		}
		delete(c.connecting, nsp)
	}
	var nsps []string
	for nsp, nc := range c.nsps {
		if nc.connected {
			nc.connected = false
			nsps = append(nsps, nsp)
		}
	}
	c.engine = nil
	closed := c.closed
	c.mu.Unlock()
	c.acks.clear()
	for _, nsp := range nsps {
		c.fireDisconnect(nsp, reason)
	}
	if !closed {
		c.reconnect(nsps)
	}
}

func (c *Client) reconnect(nsps []string) {
	if !c.opts.Reconnection || len(nsps) == 0 {
		return
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return
	}
	c.logf().Debugf("socketio: scheduling reconnection for %v", nsps)
	go c.reconnectLoop(nsps)
}

func (c *Client) reconnectLoop(nsps []string) {
	attempts := c.opts.ReconnectionAttempts
	delay := c.opts.ReconnectionDelay
	attempt := 0
	for {
		select {
		case <-c.reconnectStop:
			return
		default:
		}
		if attempts > 0 && attempt >= attempts {
			c.giveUpReconnecting(nsps)
			return
		}
		attempt++
		wait := delay
		if c.opts.RandomizationFactor > 0 {
			jitter := 1 - c.opts.RandomizationFactor + 2*c.opts.RandomizationFactor*rand.Float64()
			wait = time.Duration(float64(delay) * jitter)
		}
		if wait > c.opts.ReconnectionDelayMax {
			wait = c.opts.ReconnectionDelayMax
		}
		select {
		case <-c.reconnectStop:
			return
		case <-time.After(wait):
		}
		if err := c.reconnectAttempt(nsps); err != nil {
			c.logf().Debugf("socketio: reconnect attempt %d failed: %v", attempt, err)
			delay *= 2
			if delay > c.opts.ReconnectionDelayMax {
				delay = c.opts.ReconnectionDelayMax
			}
			continue
		}
		c.logf().Debugf("socketio: reconnected")
		return
	}
}

// reconnectAttempt re-establishes the Engine.IO session and reconnects every
// namespace that was connected before the drop.
func (c *Client) reconnectAttempt(nsps []string) (err error) {
	defer func() {
		if err != nil {
			c.mu.Lock()
			engine := c.engine
			c.engine = nil
			c.mu.Unlock()
			if engine != nil {
				_ = engine.Close()
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
	defer cancel()
	if err = c.dialEngine(ctx); err != nil {
		return err
	}
	for _, nsp := range nsps {
		if err = c.connectNsp(ctx, nsp, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) giveUpReconnecting(nsps []string) {
	err := errors.New("socketio: reconnection failed after maximum attempts")
	for _, nsp := range nsps {
		c.fireConnectError(nsp, err)
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.reconnectStopOnce.Do(func() { close(c.reconnectStop) })
}

func mapClientDisconnectReason(err error) string {
	if err == nil {
		return "transport close"
	}
	if strings.Contains(err.Error(), "ping timeout") {
		return "ping timeout"
	}
	return "transport error"
}

func newClient(rawURL string, opts *Options) *Client {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o = o.withDefaults()
	return &Client{
		rawURL:         rawURL,
		opts:           o,
		logger:         defaultLogger,
		nsps:           make(map[string]*nsClient),
		onConnect:      make(map[string][]reflect.Value),
		onEvent:        make(map[string]map[string][]reflect.Value),
		onDisconnect:   make(map[string][]reflect.Value),
		onConnectError: make(map[string][]reflect.Value),
		acks:           newAckTable(),
		connecting:     make(map[string]chan error),
		in:             newPacketQueue(),
		reconnectStop:  make(chan struct{}),
	}
}
