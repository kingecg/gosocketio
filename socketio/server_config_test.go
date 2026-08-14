package socketio

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kingecg/gosocketio/engineio"
)

// ServerConfig.Adapter accepts an AdapterFactory. The field is declared here;
// wiring it into namespace creation lands in a later todo, so this only proves
// the field exists with the right type.
var _ = ServerConfig{Adapter: func(nsp string) Adapter { return NewMemoryAdapter() }}

// doHandshake issues a polling handshake request against srv at path and
// returns the decoded open packet (or nil plus the status code on failure).
func doHandshake(t *testing.T, srv *Server, path string) (map[string]any, int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path+"?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return nil, rec.Code
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "0{") {
		t.Fatalf("handshake body = %q, want an open packet", body)
	}
	var open map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(body, "0")), &open); err != nil {
		t.Fatalf("decode open packet %q: %v", body, err)
	}
	return open, rec.Code
}

func TestServerConfigPathGuard(t *testing.T) {
	// Given a server configured with Path "/socket.io"
	srv := NewServerWithConfig(&ServerConfig{Path: "/socket.io"})
	t.Cleanup(srv.Close)

	// When a polling handshake arrives under the configured path
	open, code := doHandshake(t, srv, "/socket.io/")

	// Then it is served normally and opens a session
	if code != http.StatusOK {
		t.Fatalf("handshake under /socket.io/: code = %d, want 200", code)
	}
	if sid, _ := open["sid"].(string); sid == "" {
		t.Fatalf("open packet missing sid: %v", open)
	}

	// When a request arrives outside the configured path
	req := httptest.NewRequest(http.MethodGet, "/other?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	// Then it is rejected with 404
	if rec.Code != http.StatusNotFound {
		t.Fatalf("request to /other: code = %d, want 404", rec.Code)
	}
}

func TestServerConfigNilEqualsNewServerNil(t *testing.T) {
	// Given a server built with NewServerWithConfig(nil) and one built with
	// NewServer(nil)
	cfgNil := NewServerWithConfig(nil)
	t.Cleanup(cfgNil.Close)
	plain := NewServer(nil)
	t.Cleanup(plain.Close)

	// When a handshake arrives at the root and at an arbitrary path
	for _, p := range []string{"/", "/anything"} {
		if _, code := doHandshake(t, cfgNil, p); code != http.StatusOK {
			t.Fatalf("NewServerWithConfig(nil): handshake at %s: code = %d, want 200", p, code)
		}
		if _, code := doHandshake(t, plain, p); code != http.StatusOK {
			t.Fatalf("NewServer(nil): handshake at %s: code = %d, want 200", p, code)
		}
	}
}

func TestServerConfigEmptyPathNoGuard(t *testing.T) {
	// Given a server configured with an empty Path
	srv := NewServerWithConfig(&ServerConfig{})
	t.Cleanup(srv.Close)

	// When a handshake arrives at any path
	// Then it is served, because "" disables the guard
	for _, p := range []string{"/", "/socket.io/", "/anything"} {
		if _, code := doHandshake(t, srv, p); code != http.StatusOK {
			t.Fatalf("empty path: handshake at %s: code = %d, want 200", p, code)
		}
	}
}

func TestServerConfigTrailingSlashPath(t *testing.T) {
	// Given a server configured with Path "/socket.io/" (trailing slash)
	srv := NewServerWithConfig(&ServerConfig{Path: "/socket.io/"})
	t.Cleanup(srv.Close)

	// When a handshake arrives under the prefix
	if _, code := doHandshake(t, srv, "/socket.io/"); code != http.StatusOK {
		t.Fatalf("handshake under /socket.io/: code = %d, want 200", code)
	}

	// When a request is outside the prefix, including a near-miss
	for _, p := range []string{"/other", "/socket.ioX"} {
		req := httptest.NewRequest(http.MethodGet, p+"?EIO=4&transport=polling", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("request to %s: code = %d, want 404", p, rec.Code)
		}
	}
}

func TestNewServerDelegatesEngineOptions(t *testing.T) {
	// Given NewServer with custom engine options
	srv := NewServer(&engineio.Options{PingInterval: 3 * time.Second})
	t.Cleanup(srv.Close)

	// When a handshake arrives
	open, code := doHandshake(t, srv, "/")
	if code != http.StatusOK {
		t.Fatalf("handshake: code = %d, want 200", code)
	}

	// Then the engine echoes the custom ping interval, proving the options
	// were delegated through NewServerWithConfig
	if got := open["pingInterval"]; got != float64(3000) {
		t.Fatalf("pingInterval = %v, want 3000", got)
	}
}

func TestAllowAllConfig(t *testing.T) {
	// When AllowAll() is called
	cfg := AllowAll()

	// Then it permits every origin and lists none explicitly
	if !cfg.AllowAll {
		t.Fatal("AllowAll() returned AllowAll=false")
	}
	if len(cfg.AllowedOrigins) != 0 {
		t.Fatalf("AllowAll() returned explicit origins %v", cfg.AllowedOrigins)
	}
}
