package socketio

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestServerOnErrorHandlerMismatch proves that when the client sends an event
// whose argument cannot be decoded into the server handler's parameter type,
// the server OnError hook fires for the packet's namespace with
// ErrHandlerMismatch.
func TestServerOnErrorHandlerMismatch(t *testing.T) {
	srv := newTestServer(t)
	errCh := make(chan error, 1)
	srv.OnError("/", func(s *Socket, err error) {
		errCh <- err
	})
	srv.OnEvent("/", "bad", func(s *Socket, n int) {})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["bad","notanint"]`)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHandlerMismatch) {
			t.Fatalf("err = %v, want ErrHandlerMismatch", err)
		}
	case <-time.After(testWait):
		t.Fatal("OnError not fired")
	}
}

// TestServerOnErrorGoodEventNoFire proves that a well-typed event does not
// trigger OnError.
func TestServerOnErrorGoodEventNoFire(t *testing.T) {
	srv := newTestServer(t)
	var fired atomic.Bool
	srv.OnError("/", func(s *Socket, err error) {
		fired.Store(true)
	})
	got := make(chan string, 1)
	srv.OnEvent("/", "good", func(s *Socket, m string) {
		got <- m
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["good","hello"]`)
	select {
	case m := <-got:
		if m != "hello" {
			t.Fatalf("got = %q", m)
		}
	case <-time.After(testWait):
		t.Fatal("no good event")
	}
	// Give any spurious OnError a chance to fire before asserting it did not.
	time.Sleep(50 * time.Millisecond)
	if fired.Load() {
		t.Fatal("OnError fired on good event")
	}
}

// TestServerOnErrorMultipleHandlers proves that every registered OnError
// handler for the namespace fires on a dispatch failure.
func TestServerOnErrorMultipleHandlers(t *testing.T) {
	srv := newTestServer(t)
	var count atomic.Int32
	srv.OnError("/", func(s *Socket, err error) {
		count.Add(1)
	})
	srv.OnError("/", func(s *Socket, err error) {
		count.Add(1)
	})
	srv.OnEvent("/", "bad", func(s *Socket, n int) {})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`2["bad","notanint"]`)
	waitFor(t, testWait, "both OnError handlers", func() bool {
		return count.Load() == 2
	})
}

// TestServerOnErrorAckOnFailingHandler proves that an ack request on a failing
// handler still produces an (empty) acknowledgement, matching existing
// behaviour, while OnError fires.
func TestServerOnErrorAckOnFailingHandler(t *testing.T) {
	srv := newTestServer(t)
	errCh := make(chan error, 1)
	srv.OnError("/", func(s *Socket, err error) {
		errCh <- err
	})
	srv.OnEvent("/", "badack", func(s *Socket, n int) {})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)

	tc.send(`27["badack","notanint"]`)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHandlerMismatch) {
			t.Fatalf("err = %v, want ErrHandlerMismatch", err)
		}
	case <-time.After(testWait):
		t.Fatal("OnError not fired")
	}
	p := tc.recvPacket(testWait)
	if p.Type != Ack || p.ID != 7 {
		t.Fatalf("got %v id=%d, want Ack id=7", p.Type, p.ID)
	}
	if p.Data == nil || len(p.Data.([]any)) != 0 {
		t.Fatalf("expected empty ack, got %#v", p.Data)
	}
}

// TestServerOnErrorReceivesSocket proves the OnError handler receives the
// *Socket of the connection that caused the dispatch error.
func TestServerOnErrorReceivesSocket(t *testing.T) {
	srv := newTestServer(t)
	errCh := make(chan error, 1)
	sidCh := make(chan string, 1)
	srv.OnError("/", func(s *Socket, err error) {
		sidCh <- s.ID()
		errCh <- err
	})
	srv.OnEvent("/", "bad", func(s *Socket, n int) {})
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

	tc.send(`2["bad","notanint"]`)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHandlerMismatch) {
			t.Fatalf("err = %v, want ErrHandlerMismatch", err)
		}
	case <-time.After(testWait):
		t.Fatal("OnError not fired")
	}
	select {
	case got := <-sidCh:
		if got != sid {
			t.Fatalf("OnError socket id = %q, want %q", got, sid)
		}
	case <-time.After(testWait):
		t.Fatal("no socket delivered to OnError")
	}
}
