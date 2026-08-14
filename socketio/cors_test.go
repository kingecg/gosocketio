package socketio

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	testAllowedOrigin    = "http://example.com"
	testDisallowedOrigin = "http://evil.example.com"
)

// newCORSServer builds a Socket.IO server from cfg and registers cleanup.
func newCORSServer(t *testing.T, cfg *ServerConfig) *Server {
	t.Helper()
	srv := NewServerWithConfig(cfg)
	t.Cleanup(srv.Close)
	return srv
}

// corsRequest issues a request against srv with the given method, path and
// Origin header, returning the recorder.
func corsRequest(t *testing.T, srv *Server, method, path, origin string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestCORSPreflightAllowed(t *testing.T) {
	// Given a server with CORS restricted to example.com
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When an OPTIONS preflight arrives from an allowed origin
	req := httptest.NewRequest(http.MethodOptions, "/socket.io/", nil)
	req.Header.Set("Origin", testAllowedOrigin)
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "content-type,authorization")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	// Then it is answered with 204 and the CORS preflight headers
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight code = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != testAllowedOrigin {
		t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, testAllowedOrigin)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET,POST" {
		t.Fatalf("Access-Control-Allow-Methods = %q, want GET,POST", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "content-type,authorization" {
		t.Fatalf("Access-Control-Allow-Headers = %q, want the echoed request headers", got)
	}
	if got := rec.Header().Get("Access-Control-Max-Age"); got != "86400" {
		t.Fatalf("Access-Control-Max-Age = %q, want 86400", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("Vary = %q, want Origin", got)
	}
}

func TestCORSPreflightDisallowed(t *testing.T) {
	// Given a server with CORS restricted to example.com
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When an OPTIONS preflight arrives from a disallowed origin
	rec := corsRequest(t, srv, http.MethodOptions, "/socket.io/", testDisallowedOrigin)

	// Then it is rejected with 403 and no Access-Control-Allow-Origin header
	if rec.Code != http.StatusForbidden {
		t.Fatalf("preflight code = %d, want 403", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none", got)
	}
}

func TestCORSPollingAllowed(t *testing.T) {
	// Given a server with CORS restricted to example.com
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When a polling handshake arrives from an allowed origin
	rec := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", testAllowedOrigin)

	// Then it is served with the CORS response headers
	if rec.Code != http.StatusOK {
		t.Fatalf("handshake code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != testAllowedOrigin {
		t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, testAllowedOrigin)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("Vary = %q, want Origin", got)
	}
}

func TestCORSPollingDisallowed(t *testing.T) {
	// Given a server with CORS restricted to example.com
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When a polling handshake arrives from a disallowed origin
	rec := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", testDisallowedOrigin)

	// Then it is rejected with 403 before reaching the engine
	if rec.Code != http.StatusForbidden {
		t.Fatalf("handshake code = %d, want 403", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none", got)
	}
}

func TestCORSAllowAll(t *testing.T) {
	// Given a server that allows every origin
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: AllowAll(),
	})

	// When an OPTIONS preflight and a polling handshake arrive from any origin
	pre := corsRequest(t, srv, http.MethodOptions, "/socket.io/", "https://anything.example")
	if pre.Code != http.StatusNoContent {
		t.Fatalf("preflight code = %d, want 204", pre.Code)
	}
	if got := pre.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("preflight Access-Control-Allow-Origin = %q, want *", got)
	}

	get := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "https://anything.example")
	if get.Code != http.StatusOK {
		t.Fatalf("handshake code = %d, want 200", get.Code)
	}
	if got := get.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("handshake Access-Control-Allow-Origin = %q, want *", got)
	}
}

func TestCORSWebsocketOrigin(t *testing.T) {
	// Given a server with CORS restricted to example.com, mounted on a real
	// HTTP server so the WebSocket upgrade can complete
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// When a WebSocket upgrade arrives from a disallowed origin
	_, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{testDisallowedOrigin}},
	})

	// Then the handshake fails
	if err == nil {
		t.Fatal("websocket dial with disallowed origin succeeded, want handshake failure")
	}

	// When a WebSocket upgrade arrives from an allowed origin
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{testAllowedOrigin}},
	})
	if err != nil {
		t.Fatalf("websocket dial with allowed origin: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })

	// Then the Engine.IO handshake completes and the open packet arrives
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read open packet: %v", err)
	}
	if !strings.HasPrefix(string(data), "0{") {
		t.Fatalf("first websocket message = %q, want an Engine.IO open packet", data)
	}
}

func TestCORSDisabledByDefault(t *testing.T) {
	// Given a server built with NewServer(nil) (no CORS config)
	srv := NewServer(nil)
	t.Cleanup(srv.Close)

	// When a polling handshake arrives with an Origin header
	rec := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", testAllowedOrigin)
	if rec.Code != http.StatusOK {
		t.Fatalf("handshake code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none (CORS disabled)", got)
	}
	if got := rec.Header().Get("Vary"); got != "" {
		t.Fatalf("Vary = %q, want none (CORS disabled)", got)
	}

	// When an OPTIONS request arrives with an Origin header
	pre := corsRequest(t, srv, http.MethodOptions, "/socket.io/", testAllowedOrigin)
	if pre.Code == http.StatusNoContent {
		t.Fatal("OPTIONS without CORS answered 204, want the engine to reject it")
	}
	if got := pre.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("OPTIONS Access-Control-Allow-Origin = %q, want none (CORS disabled)", got)
	}
}

func TestCORSOriginMatchingCaseInsensitive(t *testing.T) {
	// Given a server whose allowed origin is written in mixed case with a port
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{"HTTPS://Sub.Example.COM:8080"}},
	})

	// When a request arrives with the same origin in a different case
	rec := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "https://sub.example.com:8080")

	// Then it is allowed and the request origin is echoed
	if rec.Code != http.StatusOK {
		t.Fatalf("handshake code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://sub.example.com:8080" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want the request origin", got)
	}

	// When a request arrives with a different port
	rec = corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "https://sub.example.com:8081")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("different-port handshake code = %d, want 403", rec.Code)
	}

	// When a request arrives with a different scheme
	rec = corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "http://sub.example.com:8080")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("different-scheme handshake code = %d, want 403", rec.Code)
	}
}

func TestCORSRejectsNullAndEmptyOrigin(t *testing.T) {
	// Given a server with CORS restricted to example.com
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When a request arrives with a "null" origin (sandboxed iframe)
	rec := corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "null")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("null-origin handshake code = %d, want 403", rec.Code)
	}

	// When a request arrives with no Origin header at all
	rec = corsRequest(t, srv, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("empty-origin handshake code = %d, want 403", rec.Code)
	}

	// But AllowAll accepts both
	all := newCORSServer(t, &ServerConfig{Path: "/socket.io/", CORS: AllowAll()})
	rec = corsRequest(t, all, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "null")
	if rec.Code != http.StatusOK {
		t.Fatalf("AllowAll null-origin handshake code = %d, want 200", rec.Code)
	}
	rec = corsRequest(t, all, http.MethodGet, "/socket.io/?EIO=4&transport=polling", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("AllowAll empty-origin handshake code = %d, want 200", rec.Code)
	}
}

func TestCORSConfiguresAcceptOptions(t *testing.T) {
	// Given a server with AllowAll CORS
	all := newCORSServer(t, &ServerConfig{CORS: AllowAll()})
	// Then the engine skips WebSocket origin verification
	if ao := all.Engine().AcceptOptions; ao == nil || !ao.InsecureSkipVerify {
		t.Fatalf("AllowAll AcceptOptions = %+v, want InsecureSkipVerify=true", ao)
	}

	// Given a server with specific allowed origins
	specific := newCORSServer(t, &ServerConfig{CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}}})
	// Then the engine enforces the origin patterns
	ao := specific.Engine().AcceptOptions
	if ao == nil {
		t.Fatal("AcceptOptions is nil")
	}
	if ao.InsecureSkipVerify {
		t.Fatal("specific-origin AcceptOptions has InsecureSkipVerify=true, want false")
	}
	if len(ao.OriginPatterns) != 1 || ao.OriginPatterns[0] != testAllowedOrigin {
		t.Fatalf("OriginPatterns = %v, want [%s]", ao.OriginPatterns, testAllowedOrigin)
	}

	// Given a server with no CORS config
	plain := NewServer(nil)
	t.Cleanup(plain.Close)
	// Then the engine's default AcceptOptions is untouched
	if ao := plain.Engine().AcceptOptions; ao == nil || !ao.InsecureSkipVerify || len(ao.OriginPatterns) != 0 {
		t.Fatalf("no-CORS AcceptOptions = %+v, want untouched engine default", ao)
	}
}

func TestCORSPathGuardRunsFirst(t *testing.T) {
	// Given a server with a path guard and CORS
	srv := newCORSServer(t, &ServerConfig{
		Path: "/socket.io/",
		CORS: &CORSConfig{AllowedOrigins: []string{testAllowedOrigin}},
	})

	// When an OPTIONS preflight arrives outside the configured path
	rec := corsRequest(t, srv, http.MethodOptions, "/other", testAllowedOrigin)

	// Then the path guard wins: 404, not a CORS response
	if rec.Code != http.StatusNotFound {
		t.Fatalf("preflight outside path code = %d, want 404", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none", got)
	}
}
