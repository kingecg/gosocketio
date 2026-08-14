package socketio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- test helpers ----

// countingReader records how many times Read is called and how many bytes it
// produced, and refuses to yield data after EOF. It proves a payload reader is
// drained exactly once at encode time and never re-read.
type countingReader struct {
	mu      sync.Mutex
	r       io.Reader
	reads   int
	bytes   int64
	drained bool
}

func (cr *countingReader) Read(p []byte) (int, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.drained {
		return 0, errors.New("countingReader: read after EOF")
	}
	n, err := cr.r.Read(p)
	cr.reads++
	cr.bytes += int64(n)
	if err == io.EOF {
		cr.drained = true
	}
	return n, err
}

func (cr *countingReader) stats() (reads int, nbytes int64) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr.reads, cr.bytes
}

// errReader always fails a read.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read boom") }

// captureWarnLogger records every Warnf call.
type captureWarnLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *captureWarnLogger) Debugf(string, ...any) {}
func (l *captureWarnLogger) Infof(string, ...any)  {}
func (l *captureWarnLogger) Errorf(string, ...any) {}
func (l *captureWarnLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *captureWarnLogger) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.warns...)
}

// ---- unit tests: hasBinary / deconstruct ----

func TestReaderHasBinary(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want bool
	}{
		{"io.Reader", strings.NewReader("x"), true},
		{"bytes.Reader", bytes.NewReader([]byte{0xff}), true},
		{"nested-in-slice", []any{"ev", strings.NewReader("x")}, true},
		{"nested-in-map", map[string]any{"k": strings.NewReader("x")}, true},
		{"deeply-nested", []any{"a", map[string]any{"b": []any{bytes.NewReader(nil)}}}, true},
		{"byte-slice", []byte("x"), true},
		{"nested-byte-slice", []any{"a", map[string]any{"b": []byte("x")}}, true},
		{"plain-string", "x", false},
		{"plain-slice", []any{"a", "b"}, false},
		{"plain-map", map[string]any{"k": "v"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasBinary(c.v); got != c.want {
				t.Fatalf("hasBinary(%#v) = %v, want %v", c.v, got, c.want)
			}
		})
	}
}

func TestReaderDeconstruct(t *testing.T) {
	t.Run("top-level-reader", func(t *testing.T) {
		var bufs [][]byte
		got, err := deconstruct([]any{"ev", strings.NewReader("hello")}, &bufs)
		if err != nil {
			t.Fatalf("deconstruct: %v", err)
		}
		if len(bufs) != 1 || string(bufs[0]) != "hello" {
			t.Fatalf("bufs = %v", bufs)
		}
		arr, ok := got.([]any)
		if !ok || arr[0] != "ev" {
			t.Fatalf("decoded = %#v", got)
		}
		ph, ok := arr[1].(placeholder)
		if !ok || !ph.Placeholder || ph.Num != 0 {
			t.Fatalf("placeholder = %#v", arr[1])
		}
	})

	t.Run("mixed-nested-roundtrip", func(t *testing.T) {
		data := []any{"ev", []byte("one"), map[string]any{"deep": bytes.NewReader([]byte{0x00, 0xff})}}
		var bufs [][]byte
		dec, err := deconstruct(data, &bufs)
		if err != nil {
			t.Fatalf("deconstruct: %v", err)
		}
		if len(bufs) != 2 {
			t.Fatalf("want 2 buffers, got %d", len(bufs))
		}
		if string(bufs[0]) != "one" {
			t.Fatalf("buf0 = %q", bufs[0])
		}
		if len(bufs[1]) != 2 || bufs[1][0] != 0x00 || bufs[1][1] != 0xff {
			t.Fatalf("buf1 = %v", bufs[1])
		}
		// Walk the JSON wire like the real encode/decode path.
		encoded, err := json.Marshal(dec)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var back any
		if err := json.Unmarshal(encoded, &back); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		recon := reconstruct(back, bufs)
		arr := recon.([]any)
		if string(arr[1].([]byte)) != "one" {
			t.Fatalf("reconstructed []byte = %q", arr[1])
		}
		m := arr[2].(map[string]any)
		if got := m["deep"].([]byte); len(got) != 2 || got[0] != 0 || got[1] != 0xff {
			t.Fatalf("reconstructed reader = %v", got)
		}
	})

	t.Run("nil-reader-empty-buffer", func(t *testing.T) {
		var bufs [][]byte
		got, err := deconstruct([]any{"ev", (*strings.Reader)(nil)}, &bufs)
		if err != nil {
			t.Fatalf("deconstruct(nil reader): %v", err)
		}
		if len(bufs) != 1 || len(bufs[0]) != 0 {
			t.Fatalf("bufs = %v", bufs)
		}
		if arr, ok := got.([]any); !ok || arr[0] != "ev" {
			t.Fatalf("decoded = %#v", got)
		}
	})

	t.Run("failing-reader-error", func(t *testing.T) {
		var bufs [][]byte
		_, err := deconstruct([]any{"ev", errReader{}}, &bufs)
		if err == nil {
			t.Fatal("want error from failing reader")
		}
		if !strings.Contains(err.Error(), "reading binary payload") {
			t.Fatalf("error = %v", err)
		}
		if len(bufs) != 0 {
			t.Fatalf("no buffer should be appended, got %d", len(bufs))
		}
	})

	t.Run("failing-reader-nested-propagates", func(t *testing.T) {
		var bufs [][]byte
		_, err := deconstruct(map[string]any{"deep": []any{errReader{}}}, &bufs)
		if err == nil {
			t.Fatal("want error from nested failing reader")
		}
	})
}

// ---- integration tests ----

// TestServerEmitReaderArgWire proves a server Socket.Emit with an io.Reader arg
// is encoded as a BINARY_EVENT with the right placeholder count and its
// content shipped as a binary attachment.
func TestServerEmitReaderArgWire(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("push", strings.NewReader("hello"))
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)
	tc.send(`2["trigger"]`)

	p := tc.recvPacket(testWait)
	if p.Type != BinaryEvent || p.Attachments != 1 {
		t.Fatalf("got %v attachments=%d", p.Type, p.Attachments)
	}
	data, ok := p.Data.([]any)
	if !ok || data[0] != "push" {
		t.Fatalf("data = %#v", p.Data)
	}
	binary := tc.recv(testWait)
	if !binary.binary || string(binary.data) != "hello" {
		t.Fatalf("binary = %v %q", binary.binary, binary.data)
	}
}

// TestServerEmitReaderArgClientReceives proves the full path: a server
// Socket.Emit with an io.Reader arg is delivered to the client handler as
// []byte.
func TestServerEmitReaderArgClientReceives(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("push", strings.NewReader("hello"))
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	got := make(chan []byte, 1)
	c.OnEvent("/", "push", func(data []byte) {
		got <- data
	})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if string(b) != "hello" {
			t.Fatalf("pushed = %q", b)
		}
	case <-time.After(testWait):
		t.Fatal("no reader push")
	}
}

// TestClientEmitReaderArgServerReceives proves the client Emit path: an
// io.Reader arg from the client arrives at the server handler as []byte.
func TestClientEmitReaderArgServerReceives(t *testing.T) {
	got := make(chan []byte, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "push", func(s *Socket, data []byte) {
		got <- data
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	if err := c.Emit("/", "push", strings.NewReader("hello")); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if string(b) != "hello" {
			t.Fatalf("received = %q", b)
		}
	case <-time.After(testWait):
		t.Fatal("no reader event at server")
	}
}

// TestReaderConsumedOnce proves the reader is drained to EOF exactly once at
// encode time: the content round-trips and any further read after EOF is
// rejected.
func TestReaderConsumedOnce(t *testing.T) {
	got := make(chan []byte, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "push", func(s *Socket, data []byte) {
		got <- data
	})
	ts := startTestServer(t, srv)

	cr := &countingReader{r: strings.NewReader("hello")}
	c := dialTestClient(t, ts.URL)
	if err := c.Emit("/", "push", cr); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if string(b) != "hello" {
			t.Fatalf("received = %q", b)
		}
	case <-time.After(testWait):
		t.Fatal("no reader event at server")
	}
	reads, nbytes := cr.stats()
	if nbytes != 5 {
		t.Fatalf("reader produced %d bytes, want 5", nbytes)
	}
	if reads == 0 {
		t.Fatal("reader was never consumed")
	}
	if crReadAfterEOF(t, cr) {
		t.Fatal("reader yielded data after EOF: it was consumed more than once")
	}
}

// crReadAfterEOF verifies the reader is exhausted: a fresh read attempt hits
// the countingReader's post-EOF guard.
func crReadAfterEOF(t *testing.T, cr *countingReader) bool {
	t.Helper()
	buf := make([]byte, 16)
	_, err := cr.Read(buf)
	return err == nil
}

// TestReaderBinaryContent proves non-UTF8 reader content round-trips byte for
// byte in both directions.
func TestReaderBinaryContent(t *testing.T) {
	payload := []byte{0x00, 0xff, 0x10, 0x80, 0x01, 0xfe}

	t.Run("server-to-client", func(t *testing.T) {
		srv := newTestServer(t)
		srv.OnEvent("/", "trigger", func(s *Socket) {
			s.Emit("push", bytes.NewReader(payload))
		})
		ts := startTestServer(t, srv)

		c := dialTestClient(t, ts.URL)
		got := make(chan []byte, 1)
		c.OnEvent("/", "push", func(data []byte) {
			got <- data
		})
		if err := c.Emit("/", "trigger"); err != nil {
			t.Fatalf("emit: %v", err)
		}
		select {
		case b := <-got:
			if !bytes.Equal(b, payload) {
				t.Fatalf("pushed = %v, want %v", b, payload)
			}
		case <-time.After(testWait):
			t.Fatal("no binary push")
		}
	})

	t.Run("client-to-server", func(t *testing.T) {
		got := make(chan []byte, 1)
		srv := newTestServer(t)
		srv.OnEvent("/", "push", func(s *Socket, data []byte) {
			got <- data
		})
		ts := startTestServer(t, srv)

		c := dialTestClient(t, ts.URL)
		if err := c.Emit("/", "push", bytes.NewReader(payload)); err != nil {
			t.Fatalf("emit: %v", err)
		}
		select {
		case b := <-got:
			if !bytes.Equal(b, payload) {
				t.Fatalf("received = %v, want %v", b, payload)
			}
		case <-time.After(testWait):
			t.Fatal("no reader event at server")
		}
	})
}

// TestReaderNestedArgs proves io.Reader args nested inside []any and
// map[string]any are binary-encoded and reconstructed.
func TestReaderNestedArgs(t *testing.T) {
	got := make(chan []any, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "multi", func(s *Socket, first []byte, second map[string]any) {
		deep, _ := second["deep"].([]byte)
		got <- []any{first, deep}
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	if err := c.Emit("/", "multi", strings.NewReader("one"), map[string]any{"deep": bytes.NewReader([]byte{0x00, 0xff})}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case v := <-got:
		if string(v[0].([]byte)) != "one" {
			t.Fatalf("first = %q", v[0])
		}
		deep, ok := v[1].([]byte)
		if !ok || len(deep) != 2 || deep[0] != 0 || deep[1] != 0xff {
			t.Fatalf("nested = %v", v[1])
		}
	case <-time.After(testWait):
		t.Fatal("no nested delivery")
	}
}

// TestReaderNilNoPanic proves a typed-nil io.Reader is treated as an empty
// payload without panicking.
func TestReaderNilNoPanic(t *testing.T) {
	got := make(chan []byte, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("empty", (*strings.Reader)(nil))
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	c.OnEvent("/", "empty", func(data []byte) {
		got <- data
	})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if len(b) != 0 {
			t.Fatalf("empty payload = %v", b)
		}
	case <-time.After(testWait):
		t.Fatal("no empty push")
	}
}

// TestReaderFailingDropped proves a failing reader drops the packet with a
// warning and leaves the connection alive.
func TestReaderFailingDropped(t *testing.T) {
	log := &captureWarnLogger{}
	srv := newTestServer(t)
	srv.SetLogger(log)
	pong := make(chan string, 1)
	srv.OnEvent("/", "bad", func(s *Socket) {
		s.Emit("never", errReader{})
	})
	srv.OnEvent("/", "ping", func(s *Socket) {
		pong <- "pong"
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	if err := c.Emit("/", "bad"); err != nil {
		t.Fatalf("emit bad: %v", err)
	}
	// Liveness: the connection must still work after the dropped packet.
	if err := c.Emit("/", "ping"); err != nil {
		t.Fatalf("emit ping: %v", err)
	}
	select {
	case <-pong:
	case <-time.After(testWait):
		t.Fatal("connection dead after failing reader emit")
	}
	// The drop warning is logged from the event handler's goroutine, so poll
	// with a deadline instead of asserting immediately (flaky otherwise).
	deadline := time.Now().Add(testWait)
	for {
		for _, w := range log.warnings() {
			if strings.Contains(w, "reading binary payload") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected drop warning, got %v", log.warnings())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestReaderEmitWithAckClient proves EmitWithAck with an io.Reader arg still
// acknowledges (client → server).
func TestReaderEmitWithAckClient(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "bin", func(s *Socket, data []byte) []byte {
		return append([]byte("pong:"), data...)
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "bin", func(args []any) {
		ack <- args
	}, strings.NewReader("hello")); err != nil {
		t.Fatalf("emit with ack: %v", err)
	}
	select {
	case args := <-ack:
		b, ok := args[0].([]byte)
		if !ok || string(b) != "pong:hello" {
			t.Fatalf("ack = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("no binary ack")
	}
}

// TestReaderEmitWithAckServer proves EmitWithAck with an io.Reader arg still
// acknowledges (server → client).
func TestReaderEmitWithAckServer(t *testing.T) {
	ackCh := make(chan []any, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "start", func(s *Socket) {
		if _, err := s.EmitWithAck("ping?", func(args []any) {
			ackCh <- args
		}, strings.NewReader("hello")); err != nil {
			t.Fatalf("emit with ack: %v", err)
		}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(testWait)
	tc.send(`2["start"]`)

	p := tc.recvPacket(testWait)
	if p.Type != BinaryEvent || p.Attachments != 1 {
		t.Fatalf("got %v attachments=%d", p.Type, p.Attachments)
	}
	data, ok := p.Data.([]any)
	if !ok || data[0] != "ping?" || p.ID < 0 {
		t.Fatalf("data = %#v id=%d", p.Data, p.ID)
	}
	binary := tc.recv(testWait)
	if !binary.binary || string(binary.data) != "hello" {
		t.Fatalf("binary = %v %q", binary.binary, binary.data)
	}

	tc.send(fmt.Sprintf(`3%d["pong!"]`, p.ID))
	select {
	case args := <-ackCh:
		if len(args) != 1 || args[0] != "pong!" {
			t.Fatalf("ack args = %#v", args)
		}
	case <-time.After(testWait):
		t.Fatal("ack not received")
	}
}

// TestReaderDoesNotChangeByteSlice is the []byte regression: raw []byte args
// still travel as binary attachments exactly as before.
func TestReaderDoesNotChangeByteSlice(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "trigger", func(s *Socket) {
		s.Emit("push", []byte{0x01, 0x02, 0x03})
	})
	ts := startTestServer(t, srv)

	c := dialTestClient(t, ts.URL)
	got := make(chan []byte, 1)
	c.OnEvent("/", "push", func(data []byte) {
		got <- data
	})
	if err := c.Emit("/", "trigger"); err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case b := <-got:
		if len(b) != 3 || b[0] != 1 || b[2] != 3 {
			t.Fatalf("pushed = %v", b)
		}
	case <-time.After(testWait):
		t.Fatal("no binary push")
	}
}
