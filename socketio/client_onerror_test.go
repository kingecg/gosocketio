package socketio

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientOnErrorHandlerMismatch proves that when the server sends an event
// whose argument cannot be decoded into the client handler's parameter type,
// the client OnError hook fires with ErrHandlerMismatch.
func TestClientOnErrorHandlerMismatch(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("bad", "notanint")
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	errCh := make(chan error, 1)
	c.OnError("/", func(err error) {
		errCh <- err
	})
	c.OnEvent("/", "bad", func(n int) {})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHandlerMismatch) {
			t.Fatalf("err = %v, want ErrHandlerMismatch", err)
		}
	case <-time.After(testWait):
		t.Fatal("OnError not fired")
	}
}

// TestClientOnErrorGoodEventNoFire proves that a well-typed event does not
// trigger OnError.
func TestClientOnErrorGoodEventNoFire(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("good", "hello")
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	var fired atomic.Bool
	c.OnError("/", func(err error) {
		fired.Store(true)
	})
	got := make(chan string, 1)
	c.OnEvent("/", "good", func(s string) {
		got <- s
	})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
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

// TestClientOnErrorMultipleHandlers proves that every registered OnError
// handler for the namespace fires on a dispatch failure.
func TestClientOnErrorMultipleHandlers(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("bad", "notanint")
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	var count atomic.Int32
	c.OnError("/", func(err error) {
		count.Add(1)
	})
	c.OnError("/", func(err error) {
		count.Add(1)
	})
	c.OnEvent("/", "bad", func(n int) {})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	waitFor(t, testWait, "both OnError handlers", func() bool {
		return count.Load() == 2
	})
}

// TestClientOnErrorAckOnFailingHandler proves that an ack request on a failing
// handler still produces an (empty) acknowledgement, matching existing
// behaviour, while OnError fires.
func TestClientOnErrorAckOnFailingHandler(t *testing.T) {
	ackCh := make(chan []any, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		if _, err := s.EmitWithAck("badack", func(args []any) {
			ackCh <- args
		}, "notanint"); err != nil {
			t.Errorf("server emit with ack: %v", err)
		}
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	errCh := make(chan error, 1)
	c.OnError("/", func(err error) {
		errCh <- err
	})
	c.OnEvent("/", "badack", func(n int) {})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHandlerMismatch) {
			t.Fatalf("err = %v, want ErrHandlerMismatch", err)
		}
	case <-time.After(testWait):
		t.Fatal("OnError not fired")
	}
	select {
	case args := <-ackCh:
		if len(args) != 0 {
			t.Fatalf("ack args = %#v, want empty", args)
		}
	case <-time.After(testWait):
		t.Fatal("no ack sent on failing handler")
	}
}
