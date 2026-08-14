package socketio

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// spyAdapter is a recording Adapter that mirrors the memory adapter's
// membership behavior so it can back a real server session, while recording
// every call the namespace makes on it. When ns is set, RemoveSocket also
// records whether the namespace still held the id — proving the
// disconnectSocket ordering invariant.
type spyAdapter struct {
	mu    sync.RWMutex
	calls []string
	ids   map[string]struct{}
	rooms map[string]map[string]struct{}
	ns    *namespace
}

func newSpyAdapter() *spyAdapter {
	return &spyAdapter{
		ids:   make(map[string]struct{}),
		rooms: make(map[string]map[string]struct{}),
	}
}

func (a *spyAdapter) record(call string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, call)
}

func (a *spyAdapter) callsSnapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.calls...)
}

func (a *spyAdapter) AddSocket(id string) {
	a.record("AddSocket(" + id + ")")
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ids[id] = struct{}{}
	// every socket automatically joins a room named after its own id
	if a.rooms[id] == nil {
		a.rooms[id] = make(map[string]struct{})
	}
	a.rooms[id][id] = struct{}{}
}

func (a *spyAdapter) RemoveSocket(id string) {
	order := ""
	if a.ns != nil {
		a.ns.mu.RLock()
		_, present := a.ns.sockets[id]
		a.ns.mu.RUnlock()
		if present {
			order = " (n.sockets still present)"
		} else {
			order = " (n.sockets already deleted)"
		}
	}
	a.record("RemoveSocket(" + id + ")" + order)
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

func (a *spyAdapter) AddToRoom(room, id string) {
	a.record("AddToRoom(" + room + "," + id + ")")
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rooms[room] == nil {
		a.rooms[room] = make(map[string]struct{})
	}
	a.rooms[room][id] = struct{}{}
}

func (a *spyAdapter) RemoveFromRoom(room, id string) {
	a.record("RemoveFromRoom(" + room + "," + id + ")")
	a.mu.Lock()
	defer a.mu.Unlock()
	if m := a.rooms[room]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(a.rooms, room)
		}
	}
}

func (a *spyAdapter) Broadcast(room string, except []string, deliver func(id string)) {
	a.record(fmt.Sprintf("Broadcast(room=%q, except=%v)", room, except))
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

func (a *spyAdapter) Sockets() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, 0, len(a.ids))
	for id := range a.ids {
		out = append(out, id)
	}
	return out
}

func (a *spyAdapter) SocketsCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.ids)
}

func (a *spyAdapter) Close() error {
	a.record("Close()")
	return nil
}

// connectClient connects a raw test client to the default namespace and
// returns its socket id.
func connectClient(t *testing.T, tc *testClient) string {
	t.Helper()
	tc.send("0")
	p := tc.recvPacket(testWait)
	if p.Type != Connect {
		t.Fatalf("expected connect, got %v", p.Type)
	}
	sid, _ := p.Data.(map[string]any)["sid"].(string)
	if sid == "" {
		t.Fatalf("missing sid in %v", p.Data)
	}
	return sid
}

// TestNamespaceAdapter_AddRemoveSocket: addSocket/removeSocket delegate to the
// adapter with the socket's id, and the id→socket map stays in sync.
func TestNamespaceAdapter_AddRemoveSocket(t *testing.T) {
	// Given a namespace backed by a spy adapter
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	ns := newNamespace(srv, "/")
	spy := newSpyAdapter()
	ns.adapter = spy

	s1 := &Socket{id: "s1", nsp: ns, server: srv}
	s2 := &Socket{id: "s2", nsp: ns, server: srv}

	// When sockets are added and removed
	ns.addSocket(s1)
	ns.addSocket(s2)
	ns.removeSocket(s1)

	// Then the adapter saw the correct calls
	calls := spy.callsSnapshot()
	want := []string{"AddSocket(s1)", "AddSocket(s2)", "RemoveSocket(s1)"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("adapter calls = %v, want %v", calls, want)
	}

	// And the id→socket map matches
	ns.mu.RLock()
	_, ok1 := ns.sockets["s1"]
	_, ok2 := ns.sockets["s2"]
	ns.mu.RUnlock()
	if ok1 {
		t.Fatal("s1 still in n.sockets after removeSocket")
	}
	if !ok2 {
		t.Fatal("s2 missing from n.sockets after addSocket")
	}
}

// TestNamespaceAdapter_RoomOps: addToRoom/removeFromRoom delegate straight to
// the adapter without touching the namespace lock.
func TestNamespaceAdapter_RoomOps(t *testing.T) {
	// Given a namespace backed by a spy adapter
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	ns := newNamespace(srv, "/")
	spy := newSpyAdapter()
	ns.adapter = spy

	s1 := &Socket{id: "s1", nsp: ns, server: srv}
	s2 := &Socket{id: "s2", nsp: ns, server: srv}

	// When room membership changes
	ns.addSocket(s1)
	ns.addSocket(s2)
	ns.addToRoom("room1", s1)
	ns.addToRoom("room1", s2)
	ns.removeFromRoom("room1", s1)

	// Then the adapter saw the correct room ops
	calls := spy.callsSnapshot()
	want := []string{
		"AddSocket(s1)", "AddSocket(s2)",
		"AddToRoom(room1,s1)", "AddToRoom(room1,s2)",
		"RemoveFromRoom(room1,s1)",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("adapter calls = %v, want %v", calls, want)
	}
}

// TestNamespaceAdapter_BroadcastDelegation: a real server session backed by a
// spy adapter — the spy sees the Broadcast(room, except) delegation and the
// deliver callback resolves ids to the right sockets (B receives, A excluded).
func TestNamespaceAdapter_BroadcastDelegation(t *testing.T) {
	// Given a real server whose default namespace is backed by a spy adapter
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	ns := srv.namespaceFor("/")
	spy := newSpyAdapter()
	ns.adapter = spy

	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	srv.OnEvent("/", "msg", func(s *Socket, room, text string) {
		s.To(room).Emit("chat", text)
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	sidA := connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	connectClient(t, clientB)

	// When both clients join room1
	for i, tc := range []*testClient{clientA, clientB} {
		tc.send(`2` + string(rune('1'+i)) + `["join","room1"]`)
		p := tc.recvPacket(testWait)
		if p.Type != Ack || !reflect.DeepEqual(p.Data, []any{"ok"}) {
			t.Fatalf("join ack got %v %#v", p.Type, p.Data)
		}
	}

	// And A broadcasts to room1
	clientA.send(`2["msg","room1","hello"]`)

	// Then B receives the event and A (the sender) does not
	p := clientB.recvPacket(testWait)
	if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
		t.Fatalf("B got %#v", p.Data)
	}
	clientA.expectSilence(300 * time.Millisecond)

	// And the spy saw the delegation with the sender excluded
	calls := spy.callsSnapshot()
	wantBroadcast := fmt.Sprintf("Broadcast(room=%q, except=[%s])", "room1", sidA)
	found := false
	for _, c := range calls {
		if c == wantBroadcast {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("adapter calls = %v, want one matching %q", calls, wantBroadcast)
	}
}

// TestNamespaceAdapter_DisconnectOrdering: disconnectSocket deletes the socket
// from n.sockets BEFORE calling adapter.RemoveSocket, so a concurrent
// broadcast can never deliver to a disconnecting socket.
func TestNamespaceAdapter_DisconnectOrdering(t *testing.T) {
	// Given a real server whose default namespace is backed by a spy adapter
	// that records whether n.sockets still held the id when RemoveSocket ran
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	ns := srv.namespaceFor("/")
	spy := newSpyAdapter()
	spy.ns = ns
	ns.adapter = spy

	disconnected := make(chan struct{})
	srv.OnDisconnect("/", func(s *Socket, reason string) {
		close(disconnected)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	sid := connectClient(t, tc)

	// When the client disconnects
	tc.send("1")

	// Then the disconnect completes
	select {
	case <-disconnected:
	case <-time.After(testWait):
		t.Fatal("disconnect not fired")
	}

	// And the spy's RemoveSocket observed n.sockets already deleted
	calls := spy.callsSnapshot()
	want := "RemoveSocket(" + sid + ") (n.sockets already deleted)"
	found := false
	for _, c := range calls {
		if c == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("adapter calls = %v, want one matching %q", calls, want)
	}
}

// TestRoomBroadcastThreeSockets: behavior preservation — with three sockets in
// a room, To() excludes the sender while BroadcastToRoom includes it.
func TestRoomBroadcastThreeSockets(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	srv.OnEvent("/", "to", func(s *Socket, room, text string) {
		s.To(room).Emit("chat", text)
	})
	srv.OnEvent("/", "broadcast-room", func(s *Socket, room, text string) {
		srv.BroadcastToRoom("/", room, "chat", text)
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	connectClient(t, clientB)
	clientC := newTestClient(t, ts.URL)
	connectClient(t, clientC)

	// All three join room1
	for i, tc := range []*testClient{clientA, clientB, clientC} {
		tc.send(`2` + string(rune('1'+i)) + `["join","room1"]`)
		p := tc.recvPacket(testWait)
		if p.Type != Ack || !reflect.DeepEqual(p.Data, []any{"ok"}) {
			t.Fatalf("join ack got %v %#v", p.Type, p.Data)
		}
	}

	// When A uses To(room): B and C receive, A does not
	clientA.send(`2["to","room1","hello"]`)
	for _, tc := range []*testClient{clientB, clientC} {
		p := tc.recvPacket(testWait)
		if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
			t.Fatalf("got %#v", p.Data)
		}
	}
	clientA.expectSilence(300 * time.Millisecond)

	// When A uses BroadcastToRoom: all three receive, including A
	clientA.send(`2["broadcast-room","room1","hi"]`)
	for _, tc := range []*testClient{clientA, clientB, clientC} {
		p := tc.recvPacket(testWait)
		if !reflect.DeepEqual(p.Data, []any{"chat", "hi"}) {
			t.Fatalf("got %#v", p.Data)
		}
	}
}
