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
	sockets     map[string]*Socket             // id -> socket
	rooms       map[string]map[string]struct{} // room -> socket ids
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
		rooms:   make(map[string]map[string]struct{}),
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

// Sockets returns a snapshot of the connected socket ids.
func (n *namespace) Sockets() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]string, 0, len(n.sockets))
	for id := range n.sockets {
		out = append(out, id)
	}
	return out
}

// SocketsCount returns the number of connected sockets.
func (n *namespace) SocketsCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.sockets)
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
	defer n.mu.Unlock()
	n.sockets[s.id] = s
	// every socket automatically joins a room named after its own id
	if n.rooms[s.id] == nil {
		n.rooms[s.id] = make(map[string]struct{})
	}
	n.rooms[s.id][s.id] = struct{}{}
}

func (n *namespace) removeSocket(s *Socket) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.sockets, s.id)
	for room, m := range n.rooms {
		if _, ok := m[s.id]; ok {
			delete(m, s.id)
			if len(m) == 0 {
				delete(n.rooms, room)
			}
		}
	}
}

func (n *namespace) addToRoom(room string, s *Socket) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.rooms[room] == nil {
		n.rooms[room] = make(map[string]struct{})
	}
	n.rooms[room][s.id] = struct{}{}
}

func (n *namespace) removeFromRoom(room string, s *Socket) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if m := n.rooms[room]; m != nil {
		delete(m, s.id)
		if len(m) == 0 {
			delete(n.rooms, room)
		}
	}
}

// broadcast sends an event to every socket in the room (or the whole
// namespace when room is empty), excluding the optional socket.
func (n *namespace) broadcast(room string, except *Socket, event string, args []any) {
	n.mu.RLock()
	var targets []*Socket
	if room != "" {
		for id := range n.rooms[room] {
			if except != nil && id == except.id {
				continue
			}
			if s, ok := n.sockets[id]; ok {
				targets = append(targets, s)
			}
		}
	} else {
		for id, s := range n.sockets {
			if except != nil && id == except.id {
				continue
			}
			targets = append(targets, s)
		}
	}
	n.mu.RUnlock()

	for _, s := range targets {
		_ = s.Emit(event, args...)
	}
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
	for room, m := range n.rooms {
		if _, ok := m[s.id]; ok {
			delete(m, s.id)
			if len(m) == 0 {
				delete(n.rooms, room)
			}
		}
	}
	disc := n.disconnect
	n.mu.Unlock()

	s.setConnected(false)
	s.acks.clear()
	if reason == "server namespace disconnect" {
		s.send(&Packet{Type: Disconnect, Nsp: n.name})
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
	except *Socket
}

// Emit sends the event to the targeted sockets.
func (b *BroadcastOperator) Emit(event string, args ...any) {
	b.ns.broadcast(b.room, b.except, event, args)
}
