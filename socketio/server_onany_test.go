package socketio

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// TestServerOnAnyFiresForNormalEvent proves that OnAny fires for a normal
// event with the correct socket, event name and decoded arguments.
func TestServerOnAnyFiresForNormalEvent(t *testing.T) {
	srv := newTestServer(t)
	type call struct {
		sid  string
		name string
		args []any
	}
	got := make(chan call, 1)
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		got <- call{sid: s.ID(), name: name, args: args}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	p := tc.recvPacket(testWait)
	if p.Type != Connect {
		t.Fatalf("expected connect, got %v", p.Type)
	}
	sid, _ := p.Data.(map[string]any)["sid"].(string)
	if sid == "" {
		t.Fatalf("missing sid in %v", p.Data)
	}

	tc.send(`2["greet","world",42]`)
	select {
	case c := <-got:
		if c.sid != sid {
			t.Fatalf("socket id = %q, want %q", c.sid, sid)
		}
		if c.name != "greet" {
			t.Fatalf("name = %q, want %q", c.name, "greet")
		}
		want := []any{"world", float64(42)}
		if !reflect.DeepEqual(c.args, want) {
			t.Fatalf("args = %#v, want %#v", c.args, want)
		}
	case <-time.After(testWait):
		t.Fatal("OnAny not fired")
	}
}

// TestServerOnAnyNoHandler proves OnAny fires for an event with no registered
// handler; the event is still delivered to OnAny.
func TestServerOnAnyNoHandler(t *testing.T) {
	srv := newTestServer(t)
	got := make(chan string, 1)
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		got <- name
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["ghost","x"]`)
	select {
	case name := <-got:
		if name != "ghost" {
			t.Fatalf("name = %q, want %q", name, "ghost")
		}
	case <-time.After(testWait):
		t.Fatal("OnAny not fired for unhandled event")
	}
}

// TestServerOnAnyMultipleHandlers proves every registered OnAny handler fires
// for a single event.
func TestServerOnAnyMultipleHandlers(t *testing.T) {
	srv := newTestServer(t)
	var count atomic.Int32
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		count.Add(1)
	})
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		count.Add(1)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["ev"]`)
	waitFor(t, testWait, "both OnAny handlers", func() bool {
		return count.Load() == 2
	})
}

// TestServerOnAnyAlongsideEventHandler proves the per-event handler still runs
// normally when an OnAny hook is registered.
func TestServerOnAnyAlongsideEventHandler(t *testing.T) {
	srv := newTestServer(t)
	anyCh := make(chan struct{}, 1)
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		anyCh <- struct{}{}
	})
	got := make(chan string, 1)
	srv.OnEvent("/", "hello", func(s *Socket, m string) {
		got <- m
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["hello","hi"]`)
	select {
	case m := <-got:
		if m != "hi" {
			t.Fatalf("event handler got %q, want %q", m, "hi")
		}
	case <-time.After(testWait):
		t.Fatal("named handler not run")
	}
	select {
	case <-anyCh:
	case <-time.After(testWait):
		t.Fatal("OnAny not fired")
	}
}

// TestServerOnAnyNotOnConnect proves OnAny does not fire for the connect
// packet.
func TestServerOnAnyNotOnConnect(t *testing.T) {
	srv := newTestServer(t)
	var fired atomic.Bool
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		fired.Store(true)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	p := tc.recvPacket(testWait)
	if p.Type != Connect {
		t.Fatalf("expected connect, got %v", p.Type)
	}
	// Give any spurious OnAny a chance to fire before asserting it did not.
	time.Sleep(50 * time.Millisecond)
	if fired.Load() {
		t.Fatal("OnAny fired on connect packet")
	}
}

// TestServerOnAnyNotOnAckOrDisconnect proves OnAny does not fire for ack or
// disconnect packets.
func TestServerOnAnyNotOnAckOrDisconnect(t *testing.T) {
	srv := newTestServer(t)
	var fired atomic.Bool
	srv.OnAny("/", func(s *Socket, name string, args []any) {
		fired.Store(true)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	// An ack with no pending request and a namespace disconnect must not
	// reach OnAny.
	tc.send(`39["ack"]`)
	tc.send("1")
	time.Sleep(50 * time.Millisecond)
	if fired.Load() {
		t.Fatal("OnAny fired on ack or disconnect packet")
	}
}
