package socketio

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Compile-time checks: NewMemoryAdapter satisfies Adapter, and the
// AdapterFactory signature is usable.
var (
	_ Adapter        = NewMemoryAdapter()
	_ AdapterFactory = func(nsp string) Adapter { return NewMemoryAdapter() }
)

// idCollector records the socket ids a Broadcast deliver callback received.
type idCollector struct {
	mu  sync.Mutex
	ids []string
}

func (c *idCollector) add(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids = append(c.ids, id)
}

func (c *idCollector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.ids...)
}

// assertDelivered fails the test unless got contains exactly want (as a set;
// map iteration makes delivery order nondeterministic).
func assertDelivered(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("delivered %v, want %v", got, want)
		}
	}
}

// TestAdapter_AddSocketAutoJoinsSelfRoom: every socket automatically joins a
// room named after its own id, so a broadcast to the whole namespace and a
// broadcast to the self-id room both reach it.
func TestAdapter_AddSocketAutoJoinsSelfRoom(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")
	a.AddSocket("s2")

	var all idCollector
	a.Broadcast("", nil, all.add)
	assertDelivered(t, all.snapshot(), "s1", "s2")

	var self idCollector
	a.Broadcast("s1", nil, self.add)
	assertDelivered(t, self.snapshot(), "s1")
}

// TestAdapter_RemoveSocketPurgesAllRooms: removing a socket drops it from the
// id set and from every room it was in.
func TestAdapter_RemoveSocketPurgesAllRooms(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")
	a.AddSocket("s2")
	a.AddToRoom("r1", "s1")
	a.AddToRoom("r2", "s1")

	a.RemoveSocket("s1")

	if got := a.SocketsCount(); got != 1 {
		t.Fatalf("SocketsCount() = %d, want 1", got)
	}
	var all idCollector
	a.Broadcast("", nil, all.add)
	assertDelivered(t, all.snapshot(), "s2")

	var r1 idCollector
	a.Broadcast("r1", nil, r1.add)
	assertDelivered(t, r1.snapshot())

	var r2 idCollector
	a.Broadcast("r2", nil, r2.add)
	assertDelivered(t, r2.snapshot())
}

// TestAdapter_BroadcastExcludesExcept: ids listed in except never receive the
// broadcast, for both room-scoped and namespace-wide broadcasts.
func TestAdapter_BroadcastExcludesExcept(t *testing.T) {
	a := NewMemoryAdapter()
	for _, id := range []string{"s1", "s2", "s3"} {
		a.AddSocket(id)
		a.AddToRoom("r", id)
	}

	var room idCollector
	a.Broadcast("r", []string{"s2"}, room.add)
	assertDelivered(t, room.snapshot(), "s1", "s3")

	var all idCollector
	a.Broadcast("", []string{"s1", "s3"}, all.add)
	assertDelivered(t, all.snapshot(), "s2")
}

// TestAdapter_DeliverCalledOutsideLock: the deliver callback must be invoked
// after the adapter released its lock. The probe tries to acquire the write
// lock from inside deliver; if Broadcast still held the read lock the write
// lock could never be acquired and the test would time out.
func TestAdapter_DeliverCalledOutsideLock(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")

	done := make(chan struct{})
	deliver := func(id string) {
		a.AddToRoom("probe", id) // blocks forever if the adapter lock is held
		close(done)
	}
	go a.Broadcast("", nil, deliver)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deliver invoked while adapter lock held (deadlock)")
	}
}

// TestAdapter_ConcurrentRoomOps: concurrent room mutations and broadcasts must
// be race-free and must not deadlock or panic (run under -race).
func TestAdapter_ConcurrentRoomOps(t *testing.T) {
	a := NewMemoryAdapter()
	const n = 16
	for i := 0; i < n; i++ {
		a.AddSocket(fmt.Sprintf("s%d", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("s%d", i)
			for j := 0; j < 50; j++ {
				a.AddToRoom("r", id)
				a.Broadcast("r", nil, func(string) {})
				a.RemoveFromRoom("r", id)
				a.Broadcast("", nil, func(string) {})
			}
		}(i)
	}
	wg.Wait()
}

// TestAdapter_SocketsAndCount: Sockets() and SocketsCount() reflect the
// adapter's current id set.
func TestAdapter_SocketsAndCount(t *testing.T) {
	a := NewMemoryAdapter()
	if got := a.SocketsCount(); got != 0 {
		t.Fatalf("SocketsCount() = %d, want 0", got)
	}
	if got := a.Sockets(); len(got) != 0 {
		t.Fatalf("Sockets() = %v, want empty", got)
	}

	a.AddSocket("s1")
	a.AddSocket("s2")
	if got := a.SocketsCount(); got != 2 {
		t.Fatalf("SocketsCount() = %d, want 2", got)
	}
	assertDelivered(t, a.Sockets(), "s1", "s2")

	a.RemoveSocket("s1")
	if got := a.SocketsCount(); got != 1 {
		t.Fatalf("SocketsCount() = %d, want 1", got)
	}
	assertDelivered(t, a.Sockets(), "s2")
}

// TestAdapter_UnknownRoomNoOp: broadcasting to a room nobody joined delivers
// to nobody.
func TestAdapter_UnknownRoomNoOp(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")

	var got idCollector
	a.Broadcast("no-such-room", nil, got.add)
	assertDelivered(t, got.snapshot())
}

// TestAdapter_CloseIdempotent: Close() is a no-op and can be called twice
// without panicking.
func TestAdapter_CloseIdempotent(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")
	if err := a.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
}

// TestAdapter_NilDeliverSafe: a nil deliver callback must not panic.
func TestAdapter_NilDeliverSafe(t *testing.T) {
	a := NewMemoryAdapter()
	a.AddSocket("s1")
	a.AddToRoom("r", "s1")
	a.Broadcast("", nil, nil)
	a.Broadcast("r", nil, nil)
	a.Broadcast("no-such-room", nil, nil)
}
