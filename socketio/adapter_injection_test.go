package socketio

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// factoryRecorder records the namespaces an AdapterFactory is invoked for and
// returns a fresh spy adapter per call so tests can also inspect the adapter
// the namespace ended up with.
type factoryRecorder struct {
	mu    sync.Mutex
	nsps  []string
	spies []*spyAdapter
}

func (f *factoryRecorder) factory(nsp string) Adapter {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nsps = append(f.nsps, nsp)
	spy := newSpyAdapter()
	f.spies = append(f.spies, spy)
	return spy
}

func (f *factoryRecorder) namespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.nsps...)
}

func (f *factoryRecorder) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.nsps)
}

// TestAdapterInjection_ConfigFactoryCreatesNamespace: a factory supplied via
// ServerConfig.Adapter is invoked to create the default namespace, receiving
// the namespace name "/".
func TestAdapterInjection_ConfigFactoryCreatesNamespace(t *testing.T) {
	// Given a server built with NewServerWithConfig carrying a recording factory
	rec := &factoryRecorder{}
	srv := NewServerWithConfig(&ServerConfig{Adapter: rec.factory})
	t.Cleanup(srv.Close)

	// Then the default namespace was created through the factory with "/"
	if got := rec.namespaces(); !reflect.DeepEqual(got, []string{"/"}) {
		t.Fatalf("factory invoked with %v, want [\"/\"]", got)
	}
}

// TestAdapterInjection_FactoryCalledOncePerNamespace: the factory is invoked
// exactly once per namespace, with the correct namespace name, and existing
// namespaces are not re-created.
func TestAdapterInjection_FactoryCalledOncePerNamespace(t *testing.T) {
	// Given a server whose namespaces are created by a recording factory
	rec := &factoryRecorder{}
	srv := NewServerWithConfig(&ServerConfig{Adapter: rec.factory})
	t.Cleanup(srv.Close)

	// When namespaces are created lazily (existing "/" is reused)
	srv.Namespace("/")
	srv.Namespace("/admin")
	srv.OnEvent("/other", "ping", func(s *Socket, m string) string { return "pong" })

	// Then the factory ran once per namespace with the right names
	if got := rec.namespaces(); !reflect.DeepEqual(got, []string{"/", "/admin", "/other"}) {
		t.Fatalf("factory invoked with %v, want [\"/\" \"/admin\" \"/other\"]", got)
	}
	if rec.count() != 3 {
		t.Fatalf("factory called %d times, want 3", rec.count())
	}
}

// TestAdapterInjection_DefaultFactoryMemoryAdapter: with no factory configured,
// NewServer still backs every namespace with the in-memory adapter and room
// broadcast keeps working (backward compatibility).
func TestAdapterInjection_DefaultFactoryMemoryAdapter(t *testing.T) {
	// Given a server built with NewServer(nil) — no factory
	srv := newTestServer(t)
	if _, ok := srv.namespaceFor("/").adapter.(*memoryAdapter); !ok {
		t.Fatal("default namespace not backed by memoryAdapter")
	}

	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	srv.OnEvent("/", "msg", func(s *Socket, room, text string) {
		s.To(room).Emit("chat", text)
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	connectClient(t, clientA)
	clientB := newTestClient(t, ts.URL)
	connectClient(t, clientB)

	// When both clients join room1 and A broadcasts to it
	for i, tc := range []*testClient{clientA, clientB} {
		tc.send(`2` + string(rune('1'+i)) + `["join","room1"]`)
		p := tc.recvPacket(testWait)
		if p.Type != Ack || !reflect.DeepEqual(p.Data, []any{"ok"}) {
			t.Fatalf("join ack got %v %#v", p.Type, p.Data)
		}
	}
	clientA.send(`2["msg","room1","hello"]`)

	// Then B receives the broadcast and A (the sender) does not
	p := clientB.recvPacket(testWait)
	if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
		t.Fatalf("B got %#v", p.Data)
	}
	clientA.expectSilence(300 * time.Millisecond)
}

// TestAdapterInjection_SetAdapterFactoryOnlyNewNamespaces: a factory set after
// namespaces already exist applies only to namespaces created afterwards.
func TestAdapterInjection_SetAdapterFactoryOnlyNewNamespaces(t *testing.T) {
	// Given a server whose default namespace already exists with the memory adapter
	srv := NewServer(nil)
	t.Cleanup(srv.Close)

	// When a factory is set after "/" was created
	rec := &factoryRecorder{}
	srv.SetAdapterFactory(rec.factory)
	srv.Namespace("/admin")

	// Then only the new namespace used the factory
	if got := rec.namespaces(); !reflect.DeepEqual(got, []string{"/admin"}) {
		t.Fatalf("factory invoked with %v, want [\"/admin\"]", got)
	}
	if _, ok := srv.namespaceFor("/").adapter.(*memoryAdapter); !ok {
		t.Fatal("pre-existing namespace lost its memory adapter")
	}
}

// TestAdapterInjection_ConfigWinsOverSetAdapterFactory: a factory configured
// through ServerConfig.Adapter takes precedence over SetAdapterFactory.
func TestAdapterInjection_ConfigWinsOverSetAdapterFactory(t *testing.T) {
	// Given a server configured with a factory, then a conflicting one set later
	rec := &factoryRecorder{}
	srv := NewServerWithConfig(&ServerConfig{Adapter: rec.factory})
	t.Cleanup(srv.Close)
	srv.SetAdapterFactory(func(nsp string) Adapter {
		t.Error("SetAdapterFactory factory must not be used when ServerConfig.Adapter is set")
		return NewMemoryAdapter()
	})

	// When a new namespace is created
	srv.Namespace("/admin")

	// Then the config factory handled it (and the default "/")
	if got := rec.namespaces(); !reflect.DeepEqual(got, []string{"/", "/admin"}) {
		t.Fatalf("config factory invoked with %v, want [\"/\" \"/admin\"]", got)
	}
}

// TestAdapterInjection_SetAdapterFactoryNilResetsToMemory: SetAdapterFactory(nil)
// selects the in-memory adapter for namespaces created afterwards.
func TestAdapterInjection_SetAdapterFactoryNilResetsToMemory(t *testing.T) {
	// Given a server with a custom factory, then reset to nil
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	srv.SetAdapterFactory(func(nsp string) Adapter { return newSpyAdapter() })
	srv.SetAdapterFactory(nil)

	// When a new namespace is created after the reset
	srv.Namespace("/x")

	// Then it is backed by the memory adapter
	if _, ok := srv.namespaceFor("/x").adapter.(*memoryAdapter); !ok {
		t.Fatal("namespace created after SetAdapterFactory(nil) not backed by memoryAdapter")
	}
}

// TestAdapterInjection_CloseCallsAdapterClose: Server.Close() releases every
// namespace's adapter by calling Close() on it.
func TestAdapterInjection_CloseCallsAdapterClose(t *testing.T) {
	// Given a server whose namespaces are backed by spy adapters
	srv := NewServerWithConfig(&ServerConfig{Adapter: func(nsp string) Adapter {
		return newSpyAdapter()
	}})
	srv.Namespace("/admin")
	rootSpy := srv.namespaceFor("/").adapter.(*spyAdapter)
	adminSpy := srv.namespaceFor("/admin").adapter.(*spyAdapter)

	// When the server closes
	srv.Close()

	// Then every namespace's adapter Close() was called
	for name, spy := range map[string]*spyAdapter{"/": rootSpy, "/admin": adminSpy} {
		found := false
		for _, c := range spy.callsSnapshot() {
			if c == "Close()" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("adapter for namespace %s never received Close()", name)
		}
	}
}
