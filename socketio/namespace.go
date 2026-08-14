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
	events      map[string][]reflect.Value
}

func newNamespace(server *Server, name string) *namespace {
	return &namespace{
		name:    name,
		server:  server,
		sockets: make(map[string]*Socket),
		adapter: NewMemoryAdapter(),
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

// OnEvent registers a handler for a named event.
func (n *namespace) OnEvent(event string, f any) {
	v := reflect.ValueOf(f)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events[event] = append(n.events[event], v)
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
