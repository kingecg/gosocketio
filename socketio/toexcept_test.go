package socketio

import (
	"reflect"
	"testing"
	"time"
)

// joinRoom sends a join event for room and waits for the ack, so membership
// is registered before any broadcast.
func joinRoom(t *testing.T, tc *testClient, ackID byte, room string) {
	t.Helper()
	tc.send(`2` + string(rune('1'+ackID)) + `["join","` + room + `"]`)
	p := tc.recvPacket(testWait)
	if p.Type != Ack || !reflect.DeepEqual(p.Data, []any{"ok"}) {
		t.Fatalf("join ack got %v %#v", p.Type, p.Data)
	}
}

// TestServerToExcept: Server.ToExcept excludes only the listed ids; the other
// members of the room still receive.
func TestServerToExcept(t *testing.T) {
	// Given a server with a join handler and three clients in room1
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	sidA := connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	connectClient(t, clientB)
	clientC := newTestClient(t, ts.URL)
	connectClient(t, clientC)

	for i, tc := range []*testClient{clientA, clientB, clientC} {
		joinRoom(t, tc, byte(i), "room1")
	}

	// When the server broadcasts to room1 excluding A
	if err := srv.ToExcept("/", "room1", []string{sidA}, "chat", "hello"); err != nil {
		t.Fatalf("ToExcept: %v", err)
	}

	// Then B and C receive, A does not
	for _, tc := range []*testClient{clientB, clientC} {
		p := tc.recvPacket(testWait)
		if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
			t.Fatalf("got %#v", p.Data)
		}
	}
	clientA.expectSilence(300 * time.Millisecond)
}

// TestBroadcastOperatorExceptChains: Except returns a new operator that can be
// chained before Emit, excluding the listed ids.
func TestBroadcastOperatorExceptChains(t *testing.T) {
	// Given a server with three clients in room1
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	sidA := connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	sidB := connectClient(t, clientB)
	clientC := newTestClient(t, ts.URL)
	connectClient(t, clientC)

	for i, tc := range []*testClient{clientA, clientB, clientC} {
		joinRoom(t, tc, byte(i), "room1")
	}

	// When the operator excludes A and B and emits
	srv.Namespace("/").To("room1").Except(sidA, sidB).Emit("chat", "hello")

	// Then only C receives
	p := clientC.recvPacket(testWait)
	if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
		t.Fatalf("C got %#v", p.Data)
	}
	clientA.expectSilence(300 * time.Millisecond)
	clientB.expectSilence(300 * time.Millisecond)
}

// TestSocketToExcept: Socket.ToExcept excludes the sender plus the listed ids.
func TestSocketToExcept(t *testing.T) {
	// Given a server with a toexcept handler and three clients in room1
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	srv.OnEvent("/", "toexcept", func(s *Socket, room, target, text string) {
		s.ToExcept(room, []string{target}).Emit("chat", text)
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	sidB := connectClient(t, clientB)
	clientC := newTestClient(t, ts.URL)
	connectClient(t, clientC)

	for i, tc := range []*testClient{clientA, clientB, clientC} {
		joinRoom(t, tc, byte(i), "room1")
	}

	// When A emits to room1 excluding B (and implicitly itself)
	clientA.send(`2["toexcept","room1","` + sidB + `","hello"]`)

	// Then only C receives; A (sender) and B (listed) do not
	p := clientC.recvPacket(testWait)
	if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
		t.Fatalf("C got %#v", p.Data)
	}
	clientA.expectSilence(300 * time.Millisecond)
	clientB.expectSilence(300 * time.Millisecond)
}

// TestToExceptEmptyExcept: an empty except list delivers to every member,
// matching To(room)/BroadcastToRoom semantics.
func TestToExceptEmptyExcept(t *testing.T) {
	// Given a server with three clients in room1
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	connectClient(t, clientB)
	clientC := newTestClient(t, ts.URL)
	connectClient(t, clientC)

	for i, tc := range []*testClient{clientA, clientB, clientC} {
		joinRoom(t, tc, byte(i), "room1")
	}

	// When the server broadcasts with an empty except list
	if err := srv.ToExcept("/", "room1", nil, "chat", "hello"); err != nil {
		t.Fatalf("ToExcept: %v", err)
	}

	// Then all three receive
	for _, tc := range []*testClient{clientA, clientB, clientC} {
		p := tc.recvPacket(testWait)
		if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
			t.Fatalf("got %#v", p.Data)
		}
	}
}

// TestServerToExceptInvalidNamespace: Server.ToExcept on an unknown namespace
// returns a non-nil error.
func TestServerToExceptInvalidNamespace(t *testing.T) {
	// Given a fresh server with no /nope namespace
	srv := newTestServer(t)
	startTestServer(t, srv)

	// When broadcasting to an unknown namespace
	err := srv.ToExcept("/nope", "room1", nil, "chat", "hello")

	// Then a non-nil error is returned
	if err == nil {
		t.Fatal("expected non-nil error for unknown namespace")
	}
}
