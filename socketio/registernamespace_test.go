package socketio

import (
	"reflect"
	"strings"
	"testing"
)

// TestRegisterNamespaceValid registers a well-formed namespace and expects
// success.
func TestRegisterNamespaceValid(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace("/admin"); err != nil {
		t.Fatalf("RegisterNamespace(/admin) = %v, want nil", err)
	}
}

// TestRegisterNamespaceNoLeadingSlash rejects a namespace that does not start
// with "/".
func TestRegisterNamespaceNoLeadingSlash(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace("admin"); err == nil {
		t.Fatal("RegisterNamespace(admin) = nil, want error")
	} else if !strings.Contains(err.Error(), "admin") {
		t.Fatalf("error %q should mention the offending namespace", err)
	}
}

// TestRegisterNamespaceEmpty rejects the empty namespace with a descriptive
// error, even though namespaceFor("") internally normalizes to "/".
func TestRegisterNamespaceEmpty(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace(""); err == nil {
		t.Fatal("RegisterNamespace(\"\") = nil, want error")
	}
}

// TestRegisterNamespaceRootRejected rejects "/" (len == 1) which is the
// implicit default namespace and must not be re-registered.
func TestRegisterNamespaceRootRejected(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace("/"); err == nil {
		t.Fatal("RegisterNamespace(\"/\") = nil, want error")
	}
}

// TestRegisterNamespaceIdempotent registers the same namespace twice and
// expects nil both times.
func TestRegisterNamespaceIdempotent(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace("/admin"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := srv.RegisterNamespace("/admin"); err != nil {
		t.Fatalf("second register: %v, want nil (idempotent)", err)
	}
}

// TestRegisterNamespaceHandlersBeforeConnect registers a namespace, attaches
// handlers after registration but before any client connects, then verifies a
// client can connect and have its event handled normally.
func TestRegisterNamespaceHandlersBeforeConnect(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.RegisterNamespace("/admin"); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Handlers attach after RegisterNamespace, before any client connects.
	srv.OnEvent("/admin", "ping", func(s *Socket, m string) string {
		return "pong:" + m
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0/admin,")
	p := tc.recvPacket(testWait)
	if p.Type != Connect || p.Nsp != "/admin" {
		t.Fatalf("got %v nsp=%q, want connect to /admin", p.Type, p.Nsp)
	}

	tc.send(`2/admin,7["ping","x"]`)
	p = tc.recvPacket(testWait)
	if p.Type != Ack || p.Nsp != "/admin" || p.ID != 7 {
		t.Fatalf("got %v nsp=%q id=%d", p.Type, p.Nsp, p.ID)
	}
	if !reflect.DeepEqual(p.Data, []any{"pong:x"}) {
		t.Fatalf("ack data = %#v, want [pong:x]", p.Data)
	}
}

// TestRegisterNamespaceImplicitCreationCompat verifies that a namespace never
// explicitly registered is still created on demand when a client connects
// (backward compatibility).
func TestRegisterNamespaceImplicitCreationCompat(t *testing.T) {
	srv := newTestServer(t)
	// No RegisterNamespace call: implicit on-demand creation must still work.
	srv.OnEvent("/implicit", "ping", func(s *Socket, m string) string {
		return "pong:" + m
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0/implicit,")
	p := tc.recvPacket(testWait)
	if p.Type != Connect || p.Nsp != "/implicit" {
		t.Fatalf("got %v nsp=%q, want connect to /implicit", p.Type, p.Nsp)
	}

	tc.send(`2/implicit,3["ping","y"]`)
	p = tc.recvPacket(testWait)
	if p.Type != Ack || p.Nsp != "/implicit" || p.ID != 3 {
		t.Fatalf("got %v nsp=%q id=%d", p.Type, p.Nsp, p.ID)
	}
	if !reflect.DeepEqual(p.Data, []any{"pong:y"}) {
		t.Fatalf("ack data = %#v, want [pong:y]", p.Data)
	}
}
