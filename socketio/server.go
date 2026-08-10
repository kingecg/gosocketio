package socketio

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"reflect"
	"sync"

	"github.com/kingecg/gosocketio/engineio"
)

// Server is a Socket.IO server layered on top of an Engine.IO server. It
// implements http.Handler.
type Server struct {
	engine *engineio.Server
	logger engineio.Logger

	mu      sync.RWMutex
	nsps    map[string]*namespace
	engines map[*engineio.Socket]*engineConn
}

// NewServer creates a Socket.IO server with its own Engine.IO server. opts
// customizes the underlying Engine.IO layer.
func NewServer(opts *engineio.Options) *Server {
	s := &Server{
		logger:  defaultLogger,
		nsps:    make(map[string]*namespace),
		engines: make(map[*engineio.Socket]*engineConn),
	}
	s.engine = engineio.NewServer(opts)
	s.engine.OnConnect(s.onEngineConnect)
	s.engine.OnData(s.onEngineData)
	s.engine.OnClose(s.onEngineClose)
	s.nsps[defaultNamespace] = newNamespace(s, defaultNamespace)
	return s
}

// Engine returns the underlying Engine.IO server.
func (s *Server) Engine() *engineio.Server { return s.engine }

// SetLogger sets the server logger.
func (s *Server) SetLogger(l engineio.Logger) {
	if l != nil {
		s.logger = l
	}
}

// ServeHTTP implements http.Handler, delegating to the Engine.IO server.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.engine.ServeHTTP(w, r)
}

// Close closes all sessions.
func (s *Server) Close() { s.engine.Close() }

// OnConnect registers a handler invoked when a client connects to a namespace.
// Returning an error rejects the connection with a CONNECT_ERROR packet.
func (s *Server) OnConnect(nsp string, f func(*Socket) error) {
	s.namespaceFor(nsp).OnConnect(f)
}

// OnDisconnect registers a handler invoked when a socket leaves a namespace.
func (s *Server) OnDisconnect(nsp string, f func(*Socket, string)) {
	s.namespaceFor(nsp).OnDisconnect(f)
}

// OnEvent registers a handler for an event within a namespace.
func (s *Server) OnEvent(nsp, event string, f any) {
	s.namespaceFor(nsp).OnEvent(event, f)
}

// Use registers a connection middleware for a namespace.
func (s *Server) Use(nsp string, m Middleware) {
	s.namespaceFor(nsp).Use(m)
}

// Namespace returns the namespace, creating it on first access.
func (s *Server) Namespace(nsp string) *namespace {
	if nsp == "" {
		nsp = defaultNamespace
	}
	return s.namespaceFor(nsp)
}

// BroadcastToNamespace sends an event to every socket in a namespace.
func (s *Server) BroadcastToNamespace(nsp, event string, args ...any) {
	if ns := s.namespaceLookup(nsp); ns != nil {
		ns.broadcast("", nil, event, args)
	}
}

// BroadcastToRoom sends an event to every socket in a namespace's room.
func (s *Server) BroadcastToRoom(nsp, room, event string, args ...any) {
	if ns := s.namespaceLookup(nsp); ns != nil {
		ns.broadcast(room, nil, event, args)
	}
}

func (s *Server) namespaceFor(name string) *namespace {
	if name == "" {
		name = defaultNamespace
	}
	s.mu.RLock()
	ns := s.nsps[name]
	s.mu.RUnlock()
	if ns != nil {
		return ns
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns = s.nsps[name]; ns != nil {
		return ns
	}
	ns = newNamespace(s, name)
	s.nsps[name] = ns
	return ns
}

func (s *Server) namespaceLookup(name string) *namespace {
	if name == "" {
		name = defaultNamespace
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nsps[name]
}

// engineConn tracks the namespace sockets multiplexed over one Engine.IO
// connection. Incoming packets are fed through an ordered queue and processed
// on a single goroutine so that a blocking event handler cannot stall the
// transport read loop (and binary reconstruction stays ordered).
type engineConn struct {
	es     *engineio.Socket
	server *Server

	mu      sync.Mutex
	sockets map[string]*Socket // nsp -> socket
	recon   *reconstructor

	in      *packetQueue
	started sync.Once
}

type reconstructor struct {
	pkt  *Packet
	bufs [][]byte
}

// inPacket is a single Engine.IO message queued for processing.
type inPacket struct {
	data   []byte
	binary bool
}

// packetQueue is an unbounded FIFO of incoming packets. Pushes never block so
// the transport read loop keeps draining even while the processor goroutine
// is busy running a handler.
type packetQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*inPacket
	closed bool
}

func newPacketQueue() *packetQueue {
	q := &packetQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *packetQueue) push(p *inPacket) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, p)
	}
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *packetQueue) pop() (*inPacket, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil, false
	}
	p := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return p, true
}

func (q *packetQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (s *Server) onEngineConnect(es *engineio.Socket) {
	s.mu.Lock()
	s.engines[es] = &engineConn{es: es, server: s, sockets: make(map[string]*Socket), in: newPacketQueue()}
	s.mu.Unlock()
}

func (s *Server) connFor(es *engineio.Socket) *engineConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	ec := s.engines[es]
	if ec == nil {
		ec = &engineConn{es: es, server: s, sockets: make(map[string]*Socket), in: newPacketQueue()}
		s.engines[es] = ec
	}
	return ec
}

// start launches the per-connection processing goroutine on first use.
func (ec *engineConn) start() {
	ec.started.Do(func() { go ec.processLoop() })
}

func (ec *engineConn) processLoop() {
	for {
		p, ok := ec.in.pop()
		if !ok {
			return
		}
		ec.handleIn(p.data, p.binary)
	}
}

func (s *Server) onEngineData(es *engineio.Socket, data []byte, binary bool) {
	ec := s.connFor(es)
	ec.start()
	ec.in.push(&inPacket{data: data, binary: binary})
}

func (ec *engineConn) handleIn(data []byte, binary bool) {
	if binary {
		ec.feedBinary(data)
		return
	}
	pkt, err := Decode(data)
	if err != nil {
		ec.server.logger.Debugf("socketio: decode error: %v", err)
		return
	}
	if pkt.Type == BinaryEvent || pkt.Type == BinaryAck {
		if pkt.Attachments == 0 {
			pkt.Type = binaryToPlain(pkt.Type)
			ec.process(pkt)
			return
		}
		ec.recon = &reconstructor{pkt: pkt}
		return
	}
	ec.process(pkt)
}

func (ec *engineConn) feedBinary(data []byte) {
	rec := ec.recon
	if rec == nil {
		ec.server.logger.Debugf("socketio: unexpected binary data")
		return
	}
	rec.bufs = append(rec.bufs, data)
	if len(rec.bufs) != rec.pkt.Attachments {
		return
	}
	ec.recon = nil
	pkt := *rec.pkt
	pkt.Data = reconstruct(pkt.Data, rec.bufs)
	pkt.Type = binaryToPlain(pkt.Type)
	ec.process(&pkt)
}

func binaryToPlain(t PacketType) PacketType {
	if t == BinaryEvent {
		return Event
	}
	if t == BinaryAck {
		return Ack
	}
	return t
}

func (ec *engineConn) process(pkt *Packet) {
	switch pkt.Type {
	case Connect:
		ec.server.handleConnect(ec, pkt)
	case Disconnect:
		ec.server.handleDisconnect(ec, pkt)
	case Event:
		ec.server.handleEvent(ec, pkt)
	case Ack:
		ec.server.handleAck(ec, pkt)
	default:
		ec.server.logger.Debugf("socketio: ignoring packet %v", pkt.Type)
	}
}

// sendPacket encodes and writes a packet to the Engine.IO connection,
// splitting binary payloads into a placeholder text packet followed by the
// raw binary buffers.
func (s *Server) sendPacket(ec *engineConn, pkt *Packet) {
	if pkt.Data != nil && hasBinary(pkt.Data) {
		var bufs [][]byte
		pkt.Data = deconstruct(pkt.Data, &bufs)
		switch pkt.Type {
		case Event:
			pkt.Type = BinaryEvent
		case Ack:
			pkt.Type = BinaryAck
		}
		pkt.Attachments = len(bufs)
		ec.es.SendMessage(pkt.Encode(), false)
		for _, b := range bufs {
			ec.es.SendMessage(b, true)
		}
		return
	}
	ec.es.SendMessage(pkt.Encode(), false)
}

func (s *Server) handleConnect(ec *engineConn, pkt *Packet) {
	nsp := pkt.Nsp
	ns := s.namespaceLookup(nsp)
	if ns == nil {
		ec.sendConnectError(nsp, "Invalid namespace")
		return
	}
	sock := newSocket(ns, ec)
	if err := ns.runMiddlewares(sock, connectData(pkt.Data)); err != nil {
		s.rejectConnect(ec, sock, ns, err.Error())
		return
	}
	ns.addSocket(sock)
	ec.mu.Lock()
	ec.sockets[nsp] = sock
	ec.mu.Unlock()
	sock.setConnected(true)
	if err := ns.runConnect(sock); err != nil {
		s.rejectConnect(ec, sock, ns, err.Error())
		return
	}
	s.sendPacket(ec, &Packet{Type: Connect, Nsp: nsp, ID: -1, Data: map[string]any{"sid": sock.id}})
}

func (s *Server) rejectConnect(ec *engineConn, sock *Socket, ns *namespace, msg string) {
	s.logger.Debugf("socketio: rejecting connection to %s: %s", ns.name, msg)
	ns.removeSocket(sock)
	ec.mu.Lock()
	delete(ec.sockets, ns.name)
	ec.mu.Unlock()
	sock.setConnected(false)
	ec.sendConnectError(ns.name, msg)
}

func (ec *engineConn) sendConnectError(nsp, msg string) {
	ec.server.sendPacket(ec, &Packet{Type: ConnectError, Nsp: nsp, ID: -1, Data: map[string]any{"message": msg}})
}

func (s *Server) handleDisconnect(ec *engineConn, pkt *Packet) {
	ec.mu.Lock()
	sock := ec.sockets[pkt.Nsp]
	ec.mu.Unlock()
	if sock == nil {
		return
	}
	sock.nsp.disconnectSocket(sock, "client namespace disconnect")
}

func (s *Server) handleEvent(ec *engineConn, pkt *Packet) {
	ec.mu.Lock()
	sock := ec.sockets[pkt.Nsp]
	ec.mu.Unlock()
	if sock == nil {
		return
	}
	data, ok := pkt.Data.([]any)
	if !ok || len(data) == 0 {
		return
	}
	name, ok := data[0].(string)
	if !ok {
		return
	}
	args := data[1:]

	// Handlers run on their own goroutines so a handler that blocks (for
	// example waiting for a round-trip acknowledgement) does not stall the
	// ordered packet processing for this connection.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []any
	for _, h := range sock.nsp.handlersFor(name) {
		wg.Add(1)
		go func(h reflect.Value) {
			defer wg.Done()
			res := invokeHandler(h, sock, args)
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
			sock.send(&Packet{Type: Ack, Nsp: pkt.Nsp, ID: pkt.ID, Data: results})
		}()
	}
}

func (s *Server) handleAck(ec *engineConn, pkt *Packet) {
	ec.mu.Lock()
	sock := ec.sockets[pkt.Nsp]
	ec.mu.Unlock()
	if sock == nil {
		return
	}
	sock.handleAck(pkt)
}

func (s *Server) onEngineClose(es *engineio.Socket, reason string, _ error) {
	s.mu.Lock()
	ec := s.engines[es]
	delete(s.engines, es)
	s.mu.Unlock()
	if ec == nil {
		return
	}
	ec.in.close()
	ec.mu.Lock()
	sockets := make([]*Socket, 0, len(ec.sockets))
	for _, sock := range ec.sockets {
		sockets = append(sockets, sock)
	}
	ec.sockets = make(map[string]*Socket)
	ec.mu.Unlock()

	for _, sock := range sockets {
		sock.nsp.disconnectSocket(sock, mapDisconnectReason(reason))
	}
}

func (s *Server) detachEngineSocket(sock *Socket) {
	ec := sock.ec
	ec.mu.Lock()
	delete(ec.sockets, sock.nsp.name)
	ec.mu.Unlock()
}

// mapDisconnectReason converts an Engine.IO close reason into the
// corresponding Socket.IO disconnect reason.
func mapDisconnectReason(reason string) string {
	switch reason {
	case "client close", "transport close":
		return "transport close"
	case "transport error":
		return "transport error"
	case "ping timeout":
		return "ping timeout"
	default:
		if reason == "" {
			return "transport close"
		}
		return reason
	}
}

func connectData(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// nextSocketID generates a random namespace-scoped socket id.
func (s *Server) nextSocketID() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// newSocket creates a socket in the connecting state for a namespace.
func newSocket(ns *namespace, ec *engineConn) *Socket {
	return &Socket{
		id:     ns.server.nextSocketID(),
		nsp:    ns,
		server: ns.server,
		ec:     ec,
		acks:   newAckTable(),
	}
}

// sentinel errors exposed to users.
var (
	// ErrNotConnected is returned when emitting on a disconnected socket.
	ErrNotConnected = errors.New("socketio: socket is not connected")
)
