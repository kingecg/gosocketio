package engineio

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/kingecg/gosocketio/engineio/transport"
)

// pollingURL builds a polling endpoint URL for the given sid.
func pollingURL(tsURL, sid string) string {
	q := url.Values{}
	q.Set("EIO", "4")
	q.Set("transport", "polling")
	q.Set("sid", sid)
	return tsURL + "/socket.io/?" + q.Encode()
}

// TestErrPayloadTooLargeAlias verifies that engineio.ErrPayloadTooLarge
// aliases transport.ErrPayloadTooLarge, so errors.Is matches both levels.
func TestErrPayloadTooLargeAlias(t *testing.T) {
	// Given: a single packet whose encoded size exceeds maxPayload.
	// When: it is decoded through the transport-level API.
	_, err := transport.DecodePayloadWithLimit([]byte("4oversize"), 8)
	if err == nil {
		t.Fatal("expected error for oversized single packet")
	}
	// Then: the error matches both the engineio-level and transport-level
	// sentinels.
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("errors.Is(err, engineio.ErrPayloadTooLarge) = false, err = %v", err)
	}
	if !errors.Is(err, transport.ErrPayloadTooLarge) {
		t.Fatalf("errors.Is(err, transport.ErrPayloadTooLarge) = false, err = %v", err)
	}
}

// TestPollingPOSTOverMaxHTTPBufferSizeReturns413 verifies that a POST body
// larger than MaxHTTPBufferSize is still rejected with HTTP 413 by the hard
// gate (pre-existing behavior preserved) and that the session survives.
func TestPollingPOSTOverMaxHTTPBufferSizeReturns413(t *testing.T) {
	// Given: a polling-only server with a small MaxHTTPBufferSize and a
	// connected client.
	opts := &Options{
		PingInterval:      50 * time.Millisecond,
		PingTimeout:       200 * time.Millisecond,
		UpgradeTimeout:    2 * time.Second,
		Transports:        []string{"polling"},
		MaxHTTPBufferSize: 256,
	}
	_, ts := newEchoServer(t, opts)
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", opts)
	defer c.Close()

	// When: a raw POST carries a payload over MaxHTTPBufferSize.
	over := strings.Repeat("x", 300)
	resp, err := http.Post(pollingURL(ts.URL, c.SID()), "text/plain", strings.NewReader("4"+over))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Then: the request is hard-gated with 413.
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}

	// And: the session stays open (the 413 gate is per-request).
	received := make(chan string, 1)
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- string(data)
	})
	if err := c.SendMessage([]byte("still-alive"), false); err != nil {
		t.Fatalf("SendMessage after 413: %v", err)
	}
	select {
	case got := <-received:
		if got != "still-alive" {
			t.Fatalf("echo = %q, want %q", got, "still-alive")
		}
	case <-time.After(testWait):
		t.Fatal("session died after 413 — the gate must be per-request")
	}
}

// TestPollingDecodeErrorClosesServerSession verifies that a decode failure
// in onDataRequest routes through OnError and closes the server-side session
// (the same fatal path used for a single oversized packet, which cannot be
// produced through the HTTP gate because the gate and the decode limit share
// the same maxBufferSize value).
func TestPollingDecodeErrorClosesServerSession(t *testing.T) {
	// Given: a polling-only server whose OnClose hook records the close error.
	opts := &Options{
		PingInterval:   50 * time.Millisecond,
		PingTimeout:    200 * time.Millisecond,
		UpgradeTimeout: 2 * time.Second,
		Transports:     []string{"polling"},
	}
	srv := NewServer(opts)
	srv.SetLogger(NopLogger)
	closed := make(chan error, 1)
	srv.OnClose(func(_ *Socket, reason string, err error) {
		closed <- err
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", opts)
	defer c.Close()

	// When: a raw POST delivers a payload that cannot be decoded.
	resp, err := http.Post(pollingURL(ts.URL, c.SID()), "text/plain", strings.NewReader("zzzz"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	// Then: the server closes the session (OnError reaches the socket, which
	// tears the connection down).
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("expected a close error, got nil")
		}
	case <-time.After(testWait):
		t.Fatal("server did not close the session after a decode error")
	}
}

// TestClientPollingDecodeStillWorks proves the client-side caller
// (client_transport.go) still decodes polling payloads after switching to
// DecodePayloadWithLimit(b, 0): a normal echo round-trip over polling.
func TestClientPollingDecodeStillWorks(t *testing.T) {
	// Given: a polling-only echo server and a connected client.
	_, ts := newEchoServer(t, fastOptions([]string{"polling"}))
	c := dialAndWaitOpen(t, ts.URL+"/socket.io/", fastOptions([]string{"polling"}))
	defer c.Close()

	// When: the client sends a message and the server replies over polling.
	received := make(chan []byte, 4)
	c.OnData(func(_ *Client, data []byte, _ bool) {
		received <- append([]byte{}, data...)
	})
	payload := []byte("client-decode-ok")
	if err := c.SendMessage(payload, false); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Then: the decoded reply arrives intact.
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatalf("echo = %q, want %q", got, payload)
		}
	case <-time.After(testWait):
		t.Fatal("timeout waiting for polling echo")
	}
}

// TestPollingTransportDropTailRecoverable is intentionally absent: the
// drop-tail truncation branch cannot be reached through the polling handler
// because the 413 body gate and the DecodePayloadWithLimit limit share the
// same maxBufferSize value, so any body under the gate is also under the
// limit. The truncation semantics are covered at the DecodePayloadWithLimit
// unit level (see engineio/transport/payload_limit_test.go); the onDataRequest
// error path is covered by TestPollingDecodeErrorClosesServerSession.
