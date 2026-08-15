package socketio

import (
	"reflect"
	"sync"
)

// Middleware is invoked with the CONNECT payload before a connection to the
// namespace is accepted. Returning a non-nil error rejects the connection
// with a CONNECT_ERROR packet carrying the error message.
type Middleware func(s *Socket, data map[string]any) error

// namespace manages the sockets and rooms of a single Socket.IO namespace.
type namespace struct {
	name   string
	server *Server

	mu          sync.RWMutex
	sockets     map[string]*Socket // id -> socket (id resolution table only)
	adapter     Adapter            // authoritative room/membership backend
	middlewares []Middleware
	connect     []reflect.Value
	disconnect  []reflect.Value
	onError     []reflect.Value
	onAny       []reflect.Value
	events      map[string][]reflect.Value

	// unknownAck is the acknowledgement data returned for EVENT packets
	// that carry an ack id but have no registered handler. nil falls back
	// to an empty ack payload (the historical behaviour).
	unknownAck []any
}

func newNamespace(server *Server, name string) *namespace {
	a := NewMemoryAdapter()
	if f := server.adapterFactory; f != nil {
		a = f(name)
	}
	return &namespace{
		name:    name,
		server:  server,
		sockets: make(map[string]*Socket),
		adapter: a,
		events:  make(map[string][]reflect.Value),
	}
}

// Name returns the namespace name.
func (n *namespace) Name() string { return n.name }

// OnConnect registers a handler invoked once a client connects to the
// namespace. Returning an error rejects the connection.
func (n *namespace) OnConnect(f func(*Socket) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connect = append(n.connect, reflect.ValueOf(f))
}

// OnDisconnect registers a handler invoked when a socket leaves the namespace.
func (n *namespace) OnDisconnect(f func(*Socket, string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnect = append(n.disconnect, reflect.ValueOf(f))
}

// OnError registers a handler invoked when an event handler fails to dispatch
// (for example an argument that cannot be decoded into the handler's parameter
// type), carrying the socket and the dispatch error. It never fires for
// connect, disconnect or connect_error paths.
func (n *namespace) OnError(f func(*Socket, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onError = append(n.onError, reflect.ValueOf(f))
}

// OnAny registers a handler invoked for every event received in the
// namespace, before the named event handlers dispatch. It receives the
// socket, the event name and the decoded arguments. It fires for every EVENT
// packet — including events with no registered handler — and never for
// connect, disconnect, connect_error or acknowledgement packets.
func (n *namespace) OnAny(f func(*Socket, string, []any)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onAny = append(n.onAny, reflect.ValueOf(f))
}

// OnEvent registers a handler for a named event.
func (n *namespace) OnEvent(event string, f any) {
	v := reflect.ValueOf(f)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events[event] = append(n.events[event], v)
}

// SetUnknownEventAck sets the acknowledgement payload returned for EVENT
// packets that carry an ack id but match no registered handler. nil (the
// default) keeps the historical behaviour of acknowledging with an empty
// payload.
func (n *namespace) SetUnknownEventAck(data []any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.unknownAck = append([]any(nil), data...)
}

// unknownEventAck returns a copy of the configured unknown-event ack payload.
func (n *namespace) unknownEventAck() []any {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.unknownAck
}

// Use registers a connection middleware.
func (n *namespace) Use(m Middleware) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.middlewares = append(n.middlewares, m)
}

// Emit broadcasts an event to every socket in the namespace.
func (n *namespace) Emit(event string, args ...any) {
	n.broadcast("", nil, event, args)
}

// To returns a broadcast operator targeting a single room.
func (n *namespace) To(room string) *BroadcastOperator {
	return &BroadcastOperator{ns: n, room: room}
}

// Sockets returns a snapshot of the connected socket ids. The adapter is
// authoritative for membership.
func (n *namespace) Sockets() []string {
	return n.adapter.Sockets()
}

// SocketsCount returns the number of connected sockets.
func (n *namespace) SocketsCount() int {
	return n.adapter.SocketsCount()
}

func (n *namespace) runMiddlewares(s *Socket, data map[string]any) error {
	n.mu.RLock()
	ms := n.middlewares
	n.mu.RUnlock()
	for _, m := range ms {
		if err := m(s, data); err != nil {
			return err
		}
	}
	return nil
}

func (n *namespace) runConnect(s *Socket) error {
	n.mu.RLock()
	hs := n.connect
	n.mu.RUnlock()
	for _, h := range hs {
		outs := h.Call([]reflect.Value{reflect.ValueOf(s)})
		if len(outs) > 0 {
			if err, ok := outs[0].Interface().(error); ok && err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *namespace) handlersFor(event string) []reflect.Value {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.events[event]
}

// fireOnError invokes every registered OnError handler for the dispatch error
// on its own goroutine, so a blocking handler cannot stall packet processing.
func (n *namespace) fireOnError(s *Socket, err error) {
	n.mu.RLock()
	hs := n.onError
	n.mu.RUnlock()
	for _, h := range hs {
		go h.Call([]reflect.Value{reflect.ValueOf(s), reflect.ValueOf(err)})
	}
}

// fireOnAny invokes every registered OnAny handler for the event on its own
// goroutine, so a blocking handler cannot stall packet processing.
func (n *namespace) fireOnAny(s *Socket, name string, args []any) {
	n.mu.RLock()
	hs := n.onAny
	n.mu.RUnlock()
	for _, h := range hs {
		go h.Call([]reflect.Value{reflect.ValueOf(s), reflect.ValueOf(name), reflect.ValueOf(args)})
	}
}

func (n *namespace) addSocket(s *Socket) {
	n.mu.Lock()
	n.sockets[s.id] = s
	n.mu.Unlock()
	n.adapter.AddSocket(s.id)
}

func (n *namespace) removeSocket(s *Socket) {
	n.mu.Lock()
	delete(n.sockets, s.id)
	n.mu.Unlock()
	n.adapter.RemoveSocket(s.id)
}

func (n *namespace) addToRoom(room string, s *Socket) {
	n.adapter.AddToRoom(room, s.id)
}

func (n *namespace) removeFromRoom(room string, s *Socket) {
	n.adapter.RemoveFromRoom(room, s.id)
}

// broadcast sends an event to every socket in the room (or the whole
// namespace when room is empty), excluding the listed socket ids.
func (n *namespace) broadcast(room string, except []string, event string, args []any) {
	n.adapter.Broadcast(room, except, func(id string) {
		n.mu.RLock()
		s := n.sockets[id]
		n.mu.RUnlock()
		if s != nil {
			_ = s.Emit(event, args...)
		}
	})
}

// disconnectSocket removes the socket from the namespace, notifies the
// client when the disconnect was server-initiated, and fires the disconnect
// handlers.
func (n *namespace) disconnectSocket(s *Socket, reason string) {
	n.mu.Lock()
	if _, ok := n.sockets[s.id]; !ok {
		n.mu.Unlock()
		return
	}
	delete(n.sockets, s.id)
	disc := n.disconnect
	n.mu.Unlock()

	// Remove the id from the adapter after n.sockets: reversing the order
	// would let a concurrent broadcast deliver to a disconnecting socket.
	n.adapter.RemoveSocket(s.id)

	s.setConnected(false)
	s.acks.clear()
	if reason == "server namespace disconnect" {
		s.send(&Packet{Type: Disconnect, Nsp: n.name, ID: -1})
	}
	for _, h := range disc {
		h.Call([]reflect.Value{reflect.ValueOf(s), reflect.ValueOf(reason)})
	}
	n.server.detachEngineSocket(s)
}

// BroadcastOperator targets a subset of a namespace for broadcasting.
type BroadcastOperator struct {
	ns     *namespace
	room   string
	except []string
}

// Emit sends the event to the targeted sockets.
func (b *BroadcastOperator) Emit(event string, args ...any) {
	b.ns.broadcast(b.room, b.except, event, args)
}

// Except returns a new operator that also excludes the listed ids, chainable
// before Emit. The receiver is left unchanged.
func (b *BroadcastOperator) Except(ids ...string) *BroadcastOperator {
	except := make([]string, 0, len(b.except)+len(ids))
	except = append(except, b.except...)
	except = append(except, ids...)
	return &BroadcastOperator{ns: b.ns, room: b.room, except: except}
}
