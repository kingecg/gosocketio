package engineio

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// dialRawWS opens a bare WebSocket connection that completes the Engine.IO
// handshake but does not implement the heartbeat (it never answers pings).
// It is used to observe the server closing the session on its own.
func dialRawWS(t *testing.T, tsURL string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(tsURL + "/socket.io/?EIO=4&transport=websocket")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, wsURLString(u), nil)
	if err != nil {
		t.Fatalf("websocket.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

// drainWS reads until the connection dies. Without a reader, the peer's
// close handshake blocks for the library's close timeout, which would slow
// the tests down.
func drainWS(conn *websocket.Conn) {
	for {
		if _, _, err := conn.Read(context.Background()); err != nil {
			return
		}
	}
}

// newSentinelServer returns a websocket-only server with a fast heartbeat and
// an OnClose hook that forwards the close error to the returned channel.
func newSentinelServer(t *testing.T, opts *Options) (*Server, *httptest.Server, chan error) {
	t.Helper()
	srv := NewServer(opts)
	srv.SetLogger(NopLogger)
	closed := make(chan error, 1)
	srv.OnClose(func(_ *Socket, reason string, err error) {
		closed <- err
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts, closed
}

func TestServerCloseErrHeartbeatTimeout(t *testing.T) {
	// Given: a server with a fast heartbeat and a client that never pongs.
	_, ts, closed := newSentinelServer(t, fastOptions([]string{"websocket"}))
	conn := dialRawWS(t, ts.URL)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("read open packet: %v", err)
	}
	go drainWS(conn)

	// When: the client stops answering pings entirely.
	// Then: the session closes with ErrHeartbeatTimeout.
	select {
	case err := <-closed:
		if !errors.Is(err, ErrHeartbeatTimeout) {
			t.Fatalf("onClose err = %v, want errors.Is(err, ErrHeartbeatTimeout)", err)
		}
	case <-time.After(testWait):
		t.Fatal("timed out waiting for heartbeat timeout close")
	}
}

func TestServerCloseErrInvalidPacket(t *testing.T) {
	// Given: a websocket-only server whose heartbeat cannot race the test
	// (10s interval) and a raw client.
	opts := &Options{
		PingInterval:   10 * time.Second,
		PingTimeout:    10 * time.Second,
		UpgradeTimeout: 2 * time.Second,
		Transports:     []string{"websocket"},
	}
	_, ts, closed := newSentinelServer(t, opts)
	conn := dialRawWS(t, ts.URL)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("read open packet: %v", err)
	}

	// When: the client sends bytes that cannot be decoded as a packet.
	if err := conn.Write(ctx, websocket.MessageText, []byte("garbage")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	go drainWS(conn)

	// Then: the session closes with ErrInvalidPacket.
	select {
	case err := <-closed:
		if !errors.Is(err, ErrInvalidPacket) {
			t.Fatalf("onClose err = %v, want errors.Is(err, ErrInvalidPacket)", err)
		}
	case <-time.After(testWait):
		t.Fatal("timed out waiting for invalid-packet close")
	}
}

func TestClientCloseErrInvalidPacket(t *testing.T) {
	// Given: a raw server that completes the handshake and then sends bytes
	// that cannot be decoded as a packet, once the client is listening.
	openPkt := []byte(`0{"sid":"test","upgrades":[],"pingInterval":10000,"pingTimeout":10000,"maxPayload":1000000}`)
	sendGarbage := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
		if err := conn.Write(context.Background(), websocket.MessageText, openPkt); err != nil {
			return
		}
		select {
		case <-sendGarbage:
		case <-time.After(testWait):
		}
		_ = conn.Write(context.Background(), websocket.MessageText, []byte("garbage"))
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := Dial(ctx, ts.URL+"/socket.io/", &Options{Transports: []string{"websocket"}})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	closed := make(chan error, 1)
	c.OnClose(func(_ *Client, err error) {
		closed <- err
	})
	close(sendGarbage)

	// When: the decode failure reaches the client transport.
	// Then: the client session closes with ErrInvalidPacket.
	select {
	case err := <-closed:
		if !errors.Is(err, ErrInvalidPacket) {
			t.Fatalf("client onClose err = %v, want errors.Is(err, ErrInvalidPacket)", err)
		}
	case <-time.After(testWait):
		t.Fatal("timed out waiting for client invalid-packet close")
	}
}
