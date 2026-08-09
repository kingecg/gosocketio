package socketio

import (
	"sync"

	"github.com/kingecg/gosocketio/engineio"
)

// Socket is a single Socket.IO connection to a namespace. It is created when
// a client sends a CONNECT packet for the namespace and stays valid until the
// client or server disconnects that namespace.
type Socket struct {
	id     string
	nsp    *namespace
	server *Server
	ec     *engineConn

	mu        sync.RWMutex
	connected bool
	acks      *ackTable
}

// ID returns the namespace-scoped socket id.
func (s *Socket) ID() string { return s.id }

// Nsp returns the namespace this socket is connected to.
func (s *Socket) Nsp() string { return s.nsp.name }

// Connected reports whether the socket is still connected.
func (s *Socket) Connected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// Engine returns the underlying Engine.IO connection.
func (s *Socket) Engine() *engineio.Socket { return s.ec.es }

// RemoteAddr returns the remote address of the underlying connection.
func (s *Socket) RemoteAddr() string {
	if es := s.ec.es; es != nil {
		return es.RemoteAddr()
	}
	return ""
}

// Emit sends an event to this socket.
func (s *Socket) Emit(event string, args ...any) error {
	if !s.Connected() {
		return ErrNotConnected
	}
	s.emitEvent(-1, event, args)
	return nil
}

// EmitWithAck sends an event that requires an acknowledgement. cb is invoked
// with the client's reply once it arrives.
func (s *Socket) EmitWithAck(event string, cb func(args []any), args ...any) (int64, error) {
	if !s.Connected() {
		return -1, ErrNotConnected
	}
	if cb == nil {
		s.emitEvent(-1, event, args)
		return -1, nil
	}
	id := s.acks.next()
	s.acks.register(id, cb)
	s.emitEvent(id, event, args)
	return id, nil
}

func (s *Socket) emitEvent(id int64, event string, args []any) {
	s.send(&Packet{Type: Event, Nsp: s.nsp.name, ID: id, Data: append([]any{event}, args...)})
}

// send hands a packet to the underlying Engine.IO connection, handling binary
// deconstruction when the payload contains binary values.
func (s *Socket) send(p *Packet) {
	s.server.sendPacket(s.ec, p)
}

// handleAck processes an acknowledgement for one of our emitted events.
func (s *Socket) handleAck(p *Packet) {
	data, _ := p.Data.([]any)
	if data == nil {
		data = []any{}
	}
	s.acks.invoke(p.ID, data)
}

// JoinRoom subscribes the socket to a room.
func (s *Socket) JoinRoom(room string) {
	s.nsp.addToRoom(room, s)
}

// LeaveRoom unsubscribes the socket from a room.
func (s *Socket) LeaveRoom(room string) {
	s.nsp.removeFromRoom(room, s)
}

// To returns a broadcast operator targeting a single room.
func (s *Socket) To(room string) *BroadcastOperator {
	return &BroadcastOperator{ns: s.nsp, room: room, except: s}
}

// BroadcastToRoom sends an event to every socket in room except this one.
func (s *Socket) BroadcastToRoom(room, event string, args ...any) {
	s.nsp.broadcast(room, s, event, args)
}

// Broadcast sends an event to every socket in the namespace except this one.
func (s *Socket) Broadcast(event string, args ...any) {
	s.nsp.broadcast("", s, event, args)
}

// Disconnect disconnects this namespace socket, notifying the client.
func (s *Socket) Disconnect() {
	s.nsp.disconnectSocket(s, "server namespace disconnect")
}

func (s *Socket) setConnected(v bool) {
	s.mu.Lock()
	s.connected = v
	s.mu.Unlock()
}
