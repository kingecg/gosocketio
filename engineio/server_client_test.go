package engineio

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// testWait bounds per-step waits. It is generous because the suite runs under
// -race on slow machines with several test binaries in parallel.
const testWait = 30 * time.Second

func newEchoServer(t *testing.T, opts *Options) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer(opts)
	srv.SetLogger(NopLogger)
	srv.OnData(func(s *Socket, data []byte, binary bool) {
		s.SendMessage(data, binary)
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

func fastOptions(transports []string) *Options {
	return &Options{
		PingInterval:   50 * time.Millisecond,
		PingTimeout:    200 * time.Millisecond,
		UpgradeTimeout: 2 * time.Second,
		Transports:     transports,
	}
}

func dialAndWaitOpen(t *testing.T, url string, opts *Options) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := Dial(ctx, url, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

func waitFor(t *testing.T, what string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEchoPolling(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions([]string{"polling"}))
	received := make(chan string, 8)
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions([]string{"polling"}))
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- string(data)
	})
	defer c.Close()

	for i := 0; i < 5; i++ {
		msg := "hello-" + string(rune('0'+i))
		if err := c.SendMessage([]byte(msg), false); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		select {
		case got := <-received:
			if got != msg {
				t.Fatalf("echo = %q, want %q", got, msg)
			}
		case <-time.After(testWait):
			t.Fatalf("timeout waiting for echo %q", msg)
		}
	}
}

func TestEchoWebsocketUpgrade(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions(nil))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions(nil))
	defer c.Close()

	received := make(chan string, 8)
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- string(data)
	})

	// Wait for the upgrade to complete.
	waitFor(t, "websocket upgrade", 5*time.Second, func() bool {
		c.mu.Lock()
		name := ""
		if c.tport != nil {
			name = c.tport.Name()
		}
		c.mu.Unlock()
		return name == "websocket"
	})

	for i := 0; i < 5; i++ {
		msg := "ws-" + string(rune('0'+i))
		if err := c.SendMessage([]byte(msg), false); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		select {
		case got := <-received:
			if got != msg {
				t.Fatalf("echo = %q, want %q", got, msg)
			}
		case <-time.After(testWait):
			t.Fatalf("timeout waiting for echo %q", msg)
		}
	}
}

func TestEchoDirectWebsocket(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions(nil))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions([]string{"websocket"}))
	defer c.Close()

	received := make(chan string, 8)
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- string(data)
	})

	msg := "direct-ws"
	if err := c.SendMessage([]byte(msg), false); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	select {
	case got := <-received:
		if got != msg {
			t.Fatalf("echo = %q, want %q", got, msg)
		}
	case <-time.After(testWait):
		t.Fatal("timeout waiting for direct websocket echo")
	}
}

func TestBinaryEchoWebsocket(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions(nil))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions(nil))
	defer c.Close()

	received := make(chan []byte, 4)
	c.OnData(func(_ *Client, data []byte, binary bool) {
		if binary {
			received <- append([]byte{}, data...)
		}
	})

	waitFor(t, "websocket upgrade", 5*time.Second, func() bool {
		c.mu.Lock()
		name := ""
		if c.tport != nil {
			name = c.tport.Name()
		}
		c.mu.Unlock()
		return name == "websocket"
	})

	payload := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	if err := c.SendMessage(payload, true); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	select {
	case got := <-received:
		if len(got) != len(payload) {
			t.Fatalf("binary echo length = %d, want %d", len(got), len(payload))
		}
		for i := range payload {
			if got[i] != payload[i] {
				t.Fatalf("binary echo mismatch at %d: %x != %x", i, got[i], payload[i])
			}
		}
	case <-time.After(testWait):
		t.Fatal("timeout waiting for binary echo")
	}
}

func TestHeartbeatKeepsConnectionAlive(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions([]string{"polling"}))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions([]string{"polling"}))
	defer c.Close()

	// With a 50ms ping interval, the connection must survive several
	// heartbeat cycles without being closed.
	time.Sleep(400 * time.Millisecond)
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		t.Fatal("connection closed despite healthy heartbeat")
	}

	received := make(chan string, 1)
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- string(data)
	})
	if err := c.SendMessage([]byte("still-alive"), false); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	select {
	case got := <-received:
		if got != "still-alive" {
			t.Fatalf("echo = %q", got)
		}
	case <-time.After(testWait):
		t.Fatal("timeout waiting for echo after heartbeat cycles")
	}
}

func TestServerCloseNotifiesClient(t *testing.T) {
	srv, ts := newEchoServer(t, fastOptions([]string{"polling"}))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions([]string{"polling"}))

	closed := make(chan error, 1)
	c.OnClose(func(_ *Client, err error) {
		closed <- err
	})

	var sock *Socket
	waitFor(t, "server to see client", 5*time.Second, func() bool {
		srv.mu.RLock()
		defer srv.mu.RUnlock()
		for _, s := range srv.clients {
			sock = s
			return true
		}
		return false
	})
	sock.Close()

	select {
	case <-closed:
		// expected
	case <-time.After(testWait):
		t.Fatal("client was not notified of server close")
	}
}

func TestConcurrentEcho(t *testing.T) {
	_, ts := newEchoServer(t, fastOptions(nil))
	const clients = 10
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions(nil))
			defer c.Close()
			received := make(chan string, 4)
			c.OnData(func(_ *Client, data []byte, _ bool) {
				received <- string(data)
			})
			for j := 0; j < 3; j++ {
				msg := string(rune('a'+i)) + string(rune('0'+j))
				if err := c.SendMessage([]byte(msg), false); err != nil {
					t.Errorf("client %d SendMessage: %v", i, err)
					return
				}
				select {
				case got := <-received:
					if got != msg {
						t.Errorf("client %d echo = %q, want %q", i, got, msg)
						return
					}
				case <-time.After(testWait):
					t.Errorf("client %d timeout (sid=%s)", i, c.SID())
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
