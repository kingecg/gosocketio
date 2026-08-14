package socketio

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// authCapture records every CONNECT auth payload delivered to a server
// middleware, copying each map so later decode cycles cannot mutate it.
type authCapture struct {
	mu    sync.Mutex
	calls []map[string]any
}

func (a *authCapture) record(data map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make(map[string]any, len(data))
	for k, v := range data {
		cp[k] = v
	}
	a.calls = append(a.calls, cp)
}

func (a *authCapture) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

func (a *authCapture) last() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.calls) == 0 {
		return nil
	}
	return a.calls[len(a.calls)-1]
}

func assertAuthEqual(t *testing.T, got, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("auth payload = %#v, want %#v", got, want)
	}
}

func waitAuthCount(t *testing.T, a *authCapture, want int, desc string) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for time.Now().Before(deadline) {
		if a.count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (count=%d, want>=%d)", desc, a.count(), want)
}

// TestReconnectPreservesAuth verifies that the CONNECT auth payload given for
// the root namespace (via Options.Auth) and for an additional namespace (via
// ConnectNamespace) is preserved and re-sent verbatim after a forced transport
// drop and automatic reconnection.
func TestReconnectPreservesAuth(t *testing.T) {
	srv := newTestServer(t)
	rootAuth := &authCapture{}
	adminAuth := &authCapture{}
	srv.Use("/", func(_ *Socket, data map[string]any) error {
		rootAuth.record(data)
		return nil
	})
	srv.Use("/admin", func(_ *Socket, data map[string]any) error {
		adminAuth.record(data)
		return nil
	})
	ts := startTestServer(t, srv)

	rootPayload := map[string]any{"token": "root-secret", "user": "alice"}
	adminPayload := map[string]any{"token": "admin-secret", "scope": "readonly"}

	c := dialTestClientOpts(t, ts.URL, &Options{
		Transports:           []string{"websocket"},
		Auth:                 rootPayload,
		Reconnection:         true,
		ReconnectionDelay:    50 * time.Millisecond,
		ReconnectionDelayMax: 200 * time.Millisecond,
		Timeout:              5 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := c.ConnectNamespace(ctx, "/admin", adminPayload); err != nil {
		t.Fatalf("connect /admin: %v", err)
	}

	waitAuthCount(t, rootAuth, 1, "initial root CONNECT auth")
	waitAuthCount(t, adminAuth, 1, "initial /admin CONNECT auth")
	assertAuthEqual(t, rootAuth.last(), rootPayload)
	assertAuthEqual(t, adminAuth.last(), adminPayload)

	// The per-namespace auth is stored on the client before any reconnect.
	c.mu.Lock()
	if !reflect.DeepEqual(c.nsps["/"].auth, rootPayload) {
		t.Fatalf("stored root auth = %#v, want %#v", c.nsps["/"].auth, rootPayload)
	}
	if !reflect.DeepEqual(c.nsps["/admin"].auth, adminPayload) {
		t.Fatalf("stored /admin auth = %#v, want %#v", c.nsps["/admin"].auth, adminPayload)
	}
	c.mu.Unlock()

	// Force the transport drop from the server side; the client must
	// reconnect both namespaces and re-send the identical auth payloads.
	srv.engine.Close()

	waitAuthCount(t, rootAuth, 2, "root CONNECT auth after reconnect")
	waitAuthCount(t, adminAuth, 2, "/admin CONNECT auth after reconnect")
	assertAuthEqual(t, rootAuth.last(), rootPayload)
	assertAuthEqual(t, adminAuth.last(), adminPayload)

	// Both namespaces are connected again on the client.
	waitFor(t, testWait, "root nsp reconnected", func() bool { return c.Connected("/") })
	waitFor(t, testWait, "admin nsp reconnected", func() bool { return c.Connected("/admin") })
}

// TestWithDefaultsRandomizationFactor verifies the RandomizationFactor default:
// a zero value (nil options or an unset field) becomes 0.5, and the reconnect
// backoff path reads that value (it is no longer 0, which previously disabled
// jitter entirely).
func TestWithDefaultsRandomizationFactor(t *testing.T) {
	// Unit: withDefaults on a zero Options fills in 0.5.
	o := (&Options{}).withDefaults()
	if o.RandomizationFactor != 0.5 {
		t.Fatalf("withDefaults: RandomizationFactor = %v, want 0.5", o.RandomizationFactor)
	}

	srv := newTestServer(t)
	ts := startTestServer(t, srv)

	// Dial with nil opts applies the default on the live client.
	c := dialTestClientOpts(t, ts.URL, nil)
	if c.opts.RandomizationFactor != 0.5 {
		t.Fatalf("client opts after Dial(nil): RandomizationFactor = %v, want 0.5", c.opts.RandomizationFactor)
	}
	c.Close()

	// A client that does not set the field explicitly must jitter its
	// reconnect backoff: the value read by reconnectLoop is 0.5, not 0.
	c2 := dialTestClientOpts(t, ts.URL, &Options{
		Transports:           []string{"websocket"},
		Reconnection:         true,
		ReconnectionDelay:    50 * time.Millisecond,
		ReconnectionDelayMax: 200 * time.Millisecond,
		Timeout:              5 * time.Second,
	})
	if c2.opts.RandomizationFactor != 0.5 {
		t.Fatalf("client opts: RandomizationFactor = %v, want 0.5", c2.opts.RandomizationFactor)
	}

	reconnected := make(chan struct{}, 4)
	c2.OnConnect("/", func() {
		reconnected <- struct{}{}
	})
	srv.engine.Close()
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("no reconnect (backoff with RandomizationFactor=0.5)")
	}
}

// TestReconnectAuthRejectedFiresConnectError verifies the failure path: when
// the server rejects the CONNECT auth during a reconnect, the client still
// surfaces OnConnectError after exhausting its attempts, and the rejected
// CONNECTs carried the identical auth payload.
func TestReconnectAuthRejectedFiresConnectError(t *testing.T) {
	srv := newTestServer(t)
	var reject atomic.Bool
	rejected := &authCapture{}
	srv.Use("/", func(_ *Socket, data map[string]any) error {
		rejected.record(data)
		if reject.Load() {
			return errors.New("unauthorized")
		}
		return nil
	})
	ts := startTestServer(t, srv)

	auth := map[string]any{"token": "s3cret"}
	c := dialTestClientOpts(t, ts.URL, &Options{
		Transports:           []string{"websocket"},
		Auth:                 auth,
		Reconnection:         true,
		ReconnectionAttempts: 2,
		ReconnectionDelay:    30 * time.Millisecond,
		ReconnectionDelayMax: 50 * time.Millisecond,
		Timeout:              5 * time.Second,
	})

	errCh := make(chan error, 4)
	c.OnConnectError("/", func(err error) {
		errCh <- err
	})

	srv.engine.Close()
	reject.Store(true)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("nil connect_error after rejected reconnect auth")
		}
		if !strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("connect_error = %q, want server rejection message", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no connect_error after rejected reconnect auth")
	}

	// Initial CONNECT plus the two rejected reconnect attempts all carried
	// the identical auth payload.
	waitAuthCount(t, rejected, 3, "rejected reconnect CONNECT auth attempts")
	assertAuthEqual(t, rejected.last(), auth)
}
