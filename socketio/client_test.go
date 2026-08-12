package socketio

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testWait bounds per-step waits. It is generous because the suite runs under
// -race on slow machines with several test binaries in parallel.
const testWait = 30 * time.Second

func dialTestClient(t *testing.T, rawURL string) *Client {
	t.Helper()
	return dialTestClientOpts(t, rawURL, nil)
}

func dialTestClientOpts(t *testing.T, rawURL string, opts *Options) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	c, err := Dial(ctx, rawURL, opts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.SetLogger(NopLogger)
	t.Cleanup(func() { c.Close() })
	return c
}

func waitFor(t *testing.T, timeout time.Duration, desc string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestClientConnectEcho(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) {
		s.Emit("echoed", msg)
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	if !c.Connected("/") {
		t.Fatal("root not connected")
	}
	if c.ID("/") == "" {
		t.Fatal("missing root sid")
	}

	echoed := make(chan string, 1)
	c.OnEvent("/", "echoed", func(msg string) {
		echoed <- msg
	})
	if err := c.Emit("/", "echo", "hi"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case m := <-echoed:
		if m != "hi" {
			t.Fatalf("echoed = %q", m)
		}
	case <-time.After(testWait):
		t.Fatal("no echo")
	}
}

func TestClientEmitWithAck(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "double", func(s *Socket, n int) int {
		return n * 2
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "double", func(args []any) {
		ack <- args
	}, 21); err != nil {
		t.Fatalf("emit with ack: %v", err)
	}
	select {
	case args := <-ack:
		if len(args) != 1 || args[0] != float64(42) {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no ack")
	}
}

func TestClientAckHandlerReturn(t *testing.T) {
	// The server emits an event with an ack id and blocks for the reply.
	// The client handler's return value is used as the ack payload.
	srv := newTestServer(t)
	srv.OnEvent("/", "ask?", func(s *Socket) string {
		resultCh := make(chan string, 1)
		if _, err := s.EmitWithAck("ask", func(args []any) {
			if len(args) == 1 {
				if s, ok := args[0].(string); ok {
					resultCh <- "got:" + s
					return
				}
			}
			resultCh <- "wrong"
		}, "trigger"); err != nil {
			return "err"
		}
		select {
		case r := <-resultCh:
			return r
		case <-time.After(testWait):
			return "timeout"
		}
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	c.OnEvent("/", "ask", func(payload string) string {
		return "ack:" + payload
	})
	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "ask?", func(args []any) {
		ack <- args
	}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case args := <-ack:
		if len(args) != 1 || args[0] != "got:ack:trigger" {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no round-trip ack")
	}
}

func TestClientBinaryEcho(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "bin", func(s *Socket, data []byte) []byte {
		return append([]byte("pong:"), data...)
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "bin", func(args []any) {
		ack <- args
	}, []byte("hello")); err != nil {
		t.Fatalf("emit with ack: %v", err)
	}
	select {
	case args := <-ack:
		b, ok := args[0].([]byte)
		if !ok || string(b) != "pong:hello" {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no binary ack")
	}
}

func TestClientServerPushesBinary(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("push", []byte{0x01, 0x02, 0x03})
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	got := make(chan []byte, 1)
	c.OnEvent("/", "push", func(data []byte) {
		got <- data
	})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if len(b) != 3 || b[0] != 1 || b[2] != 3 {
			t.Fatalf("pushed = %v", b)
		}
	case <-time.After(testWait):
		t.Fatal("no binary push")
	}
}

func TestClientBinaryMultiAttachment(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "multi", func(s *Socket, first []byte, second map[string]any) {
		deep, _ := second["deep"].([]byte)
		s.Emit("multi2", first, deep)
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	got := make(chan []any, 1)
	c.OnEvent("/", "multi2", func(first []byte, second []byte) {
		got <- []any{first, second}
	})
	if err := c.Emit("/", "multi", []byte("one"), map[string]any{"deep": []byte("two")}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case v := <-got:
		if string(v[0].([]byte)) != "one" || string(v[1].([]byte)) != "two" {
			t.Fatalf("multi = %#v", v)
		}
	case <-time.After(testWait):
		t.Fatal("no multi-attachment delivery")
	}
}

func TestClientNamespace(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/admin", "ping", func(s *Socket, m string) string {
		return "pong:" + m
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ConnectNamespace(ctx, "/admin", nil); err != nil {
		t.Fatalf("connect namespace: %v", err)
	}
	if !c.Connected("/admin") {
		t.Fatal("admin not connected")
	}
	if c.ID("/admin") == "" {
		t.Fatal("missing admin sid")
	}

	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/admin", "ping", func(args []any) {
		ack <- args
	}, "x"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case args := <-ack:
		if len(args) != 1 || args[0] != "pong:x" {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no admin ack")
	}
}

func TestClientConnectError(t *testing.T) {
	srv := newTestServer(t)
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	errCh := make(chan error, 1)
	c.OnConnectError("/nope", func(err error) {
		errCh <- err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ConnectNamespace(ctx, "/nope", nil); err == nil {
		t.Fatal("expected connect error")
	} else if !strings.Contains(err.Error(), "Invalid namespace") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-errCh:
	case <-time.After(testWait):
		t.Fatal("connect_error handler not fired")
	}
}

func TestClientAuthReject(t *testing.T) {
	srv := newTestServer(t)
	srv.Use("/", func(s *Socket, data map[string]any) error {
		if data == nil || data["token"] != "ok" {
			return errAuth
		}
		return nil
	})
	ts := startTestServer(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := Dial(ctx, ts.URL, nil); err == nil {
		cancel()
		t.Fatal("expected dial failure without auth")
	}
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	c, err := Dial(ctx2, ts.URL, &Options{Auth: map[string]any{"token": "ok"}})
	if err != nil {
		t.Fatalf("dial with auth: %v", err)
	}
	c.SetLogger(NopLogger)
	defer c.Close()
}

func TestClientServerInitiatedDisconnect(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "kick", func(s *Socket) {
		s.Disconnect()
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	disc := make(chan string, 1)
	c.OnDisconnect("/", func(reason string) {
		disc <- reason
	})
	if err := c.Emit("/", "kick"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case reason := <-disc:
		if reason != "io server disconnect" {
			t.Fatalf("reason = %q", reason)
		}
	case <-time.After(testWait):
		t.Fatal("no disconnect")
	}
	if c.Connected("/") {
		t.Fatal("still connected after kick")
	}
	if err := c.Emit("/", "kick"); err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestClientDisconnectNamespace(t *testing.T) {
	serverReason := make(chan string, 1)
	srv := newTestServer(t)
	srv.OnDisconnect("/", func(s *Socket, reason string) {
		serverReason <- reason
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	disc := make(chan string, 1)
	c.OnDisconnect("/", func(reason string) {
		disc <- reason
	})
	c.DisconnectNamespace("/")
	select {
	case reason := <-disc:
		if reason != "io client disconnect" {
			t.Fatalf("client reason = %q", reason)
		}
	case <-time.After(testWait):
		t.Fatal("no client disconnect")
	}
	select {
	case reason := <-serverReason:
		if reason != "client namespace disconnect" {
			t.Fatalf("server reason = %q", reason)
		}
	case <-time.After(testWait):
		t.Fatal("server disconnect not fired")
	}
}

func TestClientReconnect(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) string {
		return "ack:" + msg
	})
	ts := startTestServer(t, srv)

	c := dialTestClientOpts(t, ts.URL, &Options{
		Transports:           []string{"websocket"},
		Reconnection:         true,
		ReconnectionDelay:    50 * time.Millisecond,
		ReconnectionDelayMax: 200 * time.Millisecond,
		RandomizationFactor:  0,
		Timeout:              5 * time.Second,
	})

	reconnected := make(chan struct{}, 4)
	disc := make(chan string, 4)
	c.OnConnect("/", func() {
		reconnected <- struct{}{}
	})
	c.OnDisconnect("/", func(reason string) {
		disc <- reason
	})

	// Force-close the engine session from the server side. The client
	// observes an unexpected transport close and must reconnect.
	srv.engine.Close()

	select {
	case reason := <-disc:
		if reason != "transport error" && reason != "transport close" && reason != "ping timeout" {
			t.Fatalf("reason = %q", reason)
		}
	case <-time.After(testWait):
		t.Fatal("no disconnect after server close")
	}

	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("no reconnect")
	}

	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "echo", func(args []any) {
		ack <- args
	}, "again"); err != nil {
		t.Fatalf("emit after reconnect: %v", err)
	}
	select {
	case args := <-ack:
		if len(args) != 1 || args[0] != "ack:again" {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no echo after reconnect")
	}
}

func TestClientReconnectExhaustsAttempts(t *testing.T) {
	srv := newTestServer(t)

	// Once the drop flag is set, every reconnection dial is refused at the
	// HTTP layer while the existing session is force-closed from the server
	// side so the client notices the drop.
	var drop atomic.Bool
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if drop.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		srv.ServeHTTP(w, r)
	})
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	c := dialTestClientOpts(t, ts.URL, &Options{
		Transports:           []string{"websocket"},
		Reconnection:         true,
		ReconnectionAttempts: 2,
		ReconnectionDelay:    30 * time.Millisecond,
		ReconnectionDelayMax: 50 * time.Millisecond,
		RandomizationFactor:  0,
		Timeout:              2 * time.Second,
	})

	errCh := make(chan error, 4)
	c.OnConnectError("/", func(err error) {
		errCh <- err
	})

	srv.engine.Close()
	drop.Store(true)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("nil connect_error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no connect_error after reconnection failure")
	}
	waitFor(t, 5*time.Second, "client to stop retrying", func() bool {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		return closed
	})
}

func TestClientDialRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := Dial(ctx, fmt.Sprintf("http://127.0.0.1:%d", port), nil); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestClientConcurrent(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) string {
		return "ack:" + msg
	})
	ts := startTestServer(t, srv)

	const n = 4
	clients := make([]*Client, n)
	for i := range clients {
		clients[i] = dialTestClient(t, ts.URL)
	}
	for i, c := range clients {
		ack := make(chan []any, 1)
		if _, err := c.EmitWithAck("/", "echo", func(args []any) {
			ack <- args
		}, fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("emit: %v", err)
		}
		select {
		case args := <-ack:
			if len(args) != 1 || args[0] != fmt.Sprintf("ack:m%d", i) {
				t.Fatalf("ack = %#v", args)
			}
		case <-time.After(testWait):
			t.Fatalf("client %d: no ack", i)
		}
	}
}
