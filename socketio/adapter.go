package socketio

import "sync"

// Adapter is the pluggable room and membership backend of a Socket.IO
// namespace. It is id-driven and knows nothing about *Socket: the namespace
// resolves ids to sockets when delivering. A custom Adapter (e.g. one backed
// by Redis) can be injected by the server; NewMemoryAdapter is the default
// in-process implementation.
//
// Lock discipline: Broadcast must invoke deliver only after it released its
// internal lock — the namespace's addSocket path takes the namespace lock and
// then calls adapter methods, so an adapter that delivers under its own lock
// deadlocks with that path.
type Adapter interface {
	// AddSocket registers an id and automatically joins it to a room named
	// after the id itself.
	AddSocket(id string)
	// RemoveSocket removes the id from the adapter and from every room.
	RemoveSocket(id string)
	// AddToRoom joins an id to a room.
	AddToRoom(room, id string)
	// RemoveFromRoom leaves an id from a room.
	RemoveFromRoom(room, id string)
	// Broadcast invokes deliver once per target id. An empty room means the
	// whole namespace; ids listed in except are skipped; an unknown room
	// delivers to nobody. deliver is called outside any adapter lock.
	Broadcast(room string, except []string, deliver func(id string))
	// Sockets returns the current socket ids. The adapter is authoritative
	// for membership.
	Sockets() []string
	// SocketsCount returns the number of current socket ids.
	SocketsCount() int
	// Close releases adapter resources. It must be idempotent.
	Close() error
}

// AdapterFactory creates the Adapter for a namespace, typically keyed by the
// namespace name so each namespace gets its own state.
type AdapterFactory func(nsp string) Adapter

// NewMemoryAdapter returns the default in-process Adapter: an id set, a
// room-to-ids map and a single RWMutex protecting both.
func NewMemoryAdapter() Adapter {
	return &memoryAdapter{
		ids:   make(map[string]struct{}),
		rooms: make(map[string]map[string]struct{}),
	}
}

// memoryAdapter is the default Adapter implementation.
type memoryAdapter struct {
	mu    sync.RWMutex
	ids   map[string]struct{}
	rooms map[string]map[string]struct{}
}

func (a *memoryAdapter) AddSocket(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ids[id] = struct{}{}
	// every socket automatically joins a room named after its own id
	if a.rooms[id] == nil {
		a.rooms[id] = make(map[string]struct{})
	}
	a.rooms[id][id] = struct{}{}
}

func (a *memoryAdapter) RemoveSocket(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.ids, id)
	for room, m := range a.rooms {
		if _, ok := m[id]; ok {
			delete(m, id)
			if len(m) == 0 {
				delete(a.rooms, room)
			}
		}
	}
}

func (a *memoryAdapter) AddToRoom(room, id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rooms[room] == nil {
		a.rooms[room] = make(map[string]struct{})
	}
	a.rooms[room][id] = struct{}{}
}

func (a *memoryAdapter) RemoveFromRoom(room, id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if m := a.rooms[room]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(a.rooms, room)
		}
	}
}

func (a *memoryAdapter) Broadcast(room string, except []string, deliver func(id string)) {
	a.mu.RLock()
	var targets []string
	if room != "" {
		for id := range a.rooms[room] {
			if !excepted(except, id) {
				targets = append(targets, id)
			}
		}
	} else {
		for id := range a.ids {
			if !excepted(except, id) {
				targets = append(targets, id)
			}
		}
	}
	a.mu.RUnlock()

	if deliver == nil {
		return
	}
	for _, id := range targets {
		deliver(id)
	}
}

func (a *memoryAdapter) Sockets() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, 0, len(a.ids))
	for id := range a.ids {
		out = append(out, id)
	}
	return out
}

func (a *memoryAdapter) SocketsCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.ids)
}

func (a *memoryAdapter) Close() error { return nil }

// excepted reports whether id is listed in except.
func excepted(except []string, id string) bool {
	for _, e := range except {
		if e == id {
			return true
		}
	}
	return false
}
