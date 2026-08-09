package socketio

import "sync"

// ackTable tracks callbacks registered for events emitted with an
// acknowledgement id, so that a matching Ack packet can invoke them.
type ackTable struct {
	mu  sync.Mutex
	seq int64
	cbs map[int64]func(args []any)
}

func newAckTable() *ackTable {
	return &ackTable{cbs: make(map[int64]func(args []any))}
}

// next allocates a fresh acknowledgement id.
func (t *ackTable) next() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	return t.seq
}

func (t *ackTable) register(id int64, cb func(args []any)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cbs[id] = cb
}

// invoke runs and removes the callback for id. It returns false if no
// callback is registered.
func (t *ackTable) invoke(id int64, args []any) bool {
	t.mu.Lock()
	cb, ok := t.cbs[id]
	if ok {
		delete(t.cbs, id)
	}
	t.mu.Unlock()
	if ok && cb != nil {
		cb(args)
	}
	return ok
}

// remove drops a pending callback (used on disconnect).
func (t *ackTable) remove(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.cbs, id)
}

// clear drops every pending callback.
func (t *ackTable) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cbs = make(map[int64]func(args []any))
}
