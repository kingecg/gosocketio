package socketio

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingecg/gosocketio/engineio"
)

type msg struct {
	data   []byte
	binary bool
}

// testClient drives the server through a raw Engine.IO client.
type testClient struct {
	t  *testing.T
	c  *engineio.Client
	ch chan msg
}

// newTestClient connects a client using pure HTTP long-polling (no transport
// upgrade) so the Socket.IO protocol tests are deterministic. WebSocket
// transport coverage is provided by newTestClientOpts.
func newTestClient(t *testing.T, rawURL string) *testClient {
	return newTestClientOpts(t, rawURL, &engineio.Options{Transports: []string{"polling"}})
}

func newTestClientOpts(t *testing.T, rawURL string, opts *engineio.Options) *testClient {
	t.Helper()
	ch := make(chan msg, 64)
	c, err := engineio.Dial(context.Background(), rawURL, opts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.OnData(func(_ *engineio.Client, data []byte, binary bool) {
		select {
		case ch <- msg{data: append([]byte(nil), data...), binary: binary}:
		default:
			t.Logf("dropped packet %q", data)
		}
	})
	t.Cleanup(func() { c.Close() })
	return &testClient{t: t, c: c, ch: ch}
}

func (tc *testClient) send(payload string) {
	tc.t.Helper()
	if err := tc.c.SendMessage([]byte(payload), false); err != nil {
		tc.t.Fatalf("send %q: %v", payload, err)
	}
}

func (tc *testClient) sendPacket(p *Packet) {
	tc.t.Helper()
	if err := tc.c.SendMessage(p.Encode(), false); err != nil {
		tc.t.Fatalf("send packet: %v", err)
	}
}

func (tc *testClient) recv(timeout time.Duration) msg {
	tc.t.Helper()
	select {
	case m := <-tc.ch:
		return m
	case <-time.After(timeout):
		tc.t.Fatalf("timed out waiting for packet")
		return msg{}
	}
}

func (tc *testClient) recvPacket(timeout time.Duration) *Packet {
	tc.t.Helper()
	m := tc.recv(timeout)
	if m.binary {
		tc.t.Fatalf("expected a text packet, got binary %q", m.data)
	}
	p, err := Decode(m.data)
	if err != nil {
		tc.t.Fatalf("decode %q: %v", m.data, err)
	}
	return p
}

// expectSilence asserts no packet arrives within the timeout.
func (tc *testClient) expectSilence(timeout time.Duration) {
	tc.t.Helper()
	select {
	case m := <-tc.ch:
		tc.t.Fatalf("unexpected packet %q", m.data)
	case <-time.After(timeout):
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	srv := NewServer(nil)
	t.Cleanup(srv.Close)
	return srv
}

func startTestServer(t *testing.T, srv *Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// ---- packet codec unit tests ----

func TestPacketEncodeDecode(t *testing.T) {
	cases := []struct {
		name   string
		packet *Packet
		want   string
	}{
		{"connect", &Packet{Type: Connect, Nsp: "/", ID: -1, Data: map[string]any{"sid": "x"}}, `0{"sid":"x"}`},
		{"connect-nsp", &Packet{Type: Connect, Nsp: "/admin", ID: -1, Data: map[string]any{"sid": "x"}}, `0/admin,{"sid":"x"}`},
		{"event", &Packet{Type: Event, Nsp: "/", ID: -1, Data: []any{"chat", "hello"}}, `2["chat","hello"]`},
		{"event-nsp", &Packet{Type: Event, Nsp: "/admin", ID: -1, Data: []any{"chat", "hello"}}, `2/admin,["chat","hello"]`},
		{"event-ack", &Packet{Type: Event, Nsp: "/", ID: 5, Data: []any{"ev"}}, `25["ev"]`},
		{"ack", &Packet{Type: Ack, Nsp: "/", ID: 5, Data: []any{"ok"}}, `35["ok"]`},
		{"ack-empty", &Packet{Type: Ack, Nsp: "/", ID: 5, Data: []any{}}, `35[]`},
		{"ack-nsp", &Packet{Type: Ack, Nsp: "/admin", ID: 7, Data: []any{float64(1), float64(2)}}, `3/admin,7[1,2]`},
		{"connect-error", &Packet{Type: ConnectError, Nsp: "/", ID: -1, Data: map[string]any{"message": "no"}}, `4{"message":"no"}`},
		{"connect-error-nsp", &Packet{Type: ConnectError, Nsp: "/admin", ID: -1, Data: map[string]any{"message": "no"}}, `4/admin,{"message":"no"}`},
		{"disconnect", &Packet{Type: Disconnect, Nsp: "/", ID: -1}, `1`},
		{"disconnect-nsp", &Packet{Type: Disconnect, Nsp: "/admin", ID: -1}, `1/admin,`},
		{"binary-event", &Packet{Type: BinaryEvent, Nsp: "/", ID: -1, Attachments: 2, Data: []any{"ev"}}, `52-["ev"]`},
		{"binary-event-nsp", &Packet{Type: BinaryEvent, Nsp: "/admin", ID: -1, Attachments: 1, Data: []any{"ev"}}, `51-/admin,["ev"]`},
		{"binary-ack", &Packet{Type: BinaryAck, Nsp: "/", ID: 3, Attachments: 1, Data: []any{}}, `61-3[]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(c.packet.Encode())
			if got != c.want {
				t.Fatalf("encode: got %q want %q", got, c.want)
			}
			p, err := Decode([]byte(c.want))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if p.Type != c.packet.Type {
				t.Fatalf("type: got %v want %v", p.Type, c.packet.Type)
			}
			if p.Nsp != c.packet.Nsp {
				t.Fatalf("nsp: got %q want %q", p.Nsp, c.packet.Nsp)
			}
			if p.ID != c.packet.ID {
				t.Fatalf("id: got %d want %d", p.ID, c.packet.ID)
			}
			if p.Attachments != c.packet.Attachments {
				t.Fatalf("attachments: got %d want %d", p.Attachments, c.packet.Attachments)
			}
			if !reflect.DeepEqual(p.Data, c.packet.Data) {
				t.Fatalf("data: got %#v want %#v", p.Data, c.packet.Data)
			}
		})
	}
}

func TestPacketDecodeInvalid(t *testing.T) {
	for _, s := range []string{"", "x", "22[" + "x", "9abc", `7["ev"]`, `2["connect",1]`, `3notjson`} {
		if _, err := Decode([]byte(s)); err == nil {
			t.Fatalf("expected error for %q", s)
		}
	}
}

func TestBinaryDeconstruct(t *testing.T) {
	data := []any{"ev", []byte("hello"), map[string]any{"nested": []byte{0x01, 0x02}}}
	var bufs [][]byte
	dec := deconstruct(data, &bufs)
	if len(bufs) != 2 {
		t.Fatalf("want 2 buffers, got %d", len(bufs))
	}
	if string(bufs[0]) != "hello" {
		t.Fatalf("buf0 = %q", bufs[0])
	}
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
	if string(arr[1].([]byte)) != "hello" {
		t.Fatalf("reconstructed binary = %q", arr[1])
	}
	m := arr[2].(map[string]any)
	if got := m["nested"].([]byte); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("reconstructed nested = %v", got)
	}
}

// ---- integration tests ----

func TestConnectAndEventEcho(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) {
		s.Emit("echoed", msg)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Connect {
		t.Fatalf("expected connect, got %v", p.Type)
	}
	sid, _ := p.Data.(map[string]any)["sid"].(string)
	if sid == "" {
		t.Fatalf("missing sid in %v", p.Data)
	}

	tc.send(`2["echo","hi"]`)
	p = tc.recvPacket(5 * time.Second)
	if p.Type != Event || !reflect.DeepEqual(p.Data, []any{"echoed", "hi"}) {
		t.Fatalf("got %v %#v", p.Type, p.Data)
	}
}

func TestWebsocketTransport(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) string {
		return "ack:" + msg
	})
	ts := startTestServer(t, srv)

	tc := newTestClientOpts(t, ts.URL, &engineio.Options{Transports: []string{"websocket"}})
	tc.send("0")
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Connect {
		t.Fatalf("expected connect, got %v", p.Type)
	}

	tc.send(`21["echo","ws"]`)
	p = tc.recvPacket(5 * time.Second)
	if p.Type != Ack || p.ID != 1 || !reflect.DeepEqual(p.Data, []any{"ack:ws"}) {
		t.Fatalf("got %v %#v", p.Type, p.Data)
	}
}

func TestEventAck(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "double", func(s *Socket, n int) int {
		return n * 2
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`21["double",21]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Ack || p.ID != 1 {
		t.Fatalf("got %v id=%d", p.Type, p.ID)
	}
	if !reflect.DeepEqual(p.Data, []any{float64(42)}) {
		t.Fatalf("ack data = %#v", p.Data)
	}
}

func TestAckWithoutHandler(t *testing.T) {
	srv := newTestServer(t)
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`25["nobody"]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Ack || p.ID != 5 {
		t.Fatalf("got %v id=%d", p.Type, p.ID)
	}
	if p.Data == nil || len(p.Data.([]any)) != 0 {
		t.Fatalf("expected empty ack, got %#v", p.Data)
	}
}

func TestNamespace(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/admin", "ping", func(s *Socket, m string) string {
		return "pong:" + m
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0/admin,")
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Connect || p.Nsp != "/admin" {
		t.Fatalf("got %v nsp=%q", p.Type, p.Nsp)
	}

	tc.send(`2/admin,7["ping","x"]`)
	p = tc.recvPacket(5 * time.Second)
	if p.Type != Ack || p.Nsp != "/admin" || p.ID != 7 {
		t.Fatalf("got %v nsp=%q id=%d", p.Type, p.Nsp, p.ID)
	}
	if !reflect.DeepEqual(p.Data, []any{"pong:x"}) {
		t.Fatalf("ack data = %#v", p.Data)
	}
}

func TestUnknownNamespaceRejected(t *testing.T) {
	srv := newTestServer(t)
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0/nope,")
	p := tc.recvPacket(5 * time.Second)
	if p.Type != ConnectError || p.Nsp != "/nope" {
		t.Fatalf("got %v nsp=%q", p.Type, p.Nsp)
	}
	if msg, _ := p.Data.(map[string]any)["message"].(string); msg != "Invalid namespace" {
		t.Fatalf("message = %q", msg)
	}
}

func TestMiddlewareReject(t *testing.T) {
	srv := newTestServer(t)
	srv.Use("/", func(s *Socket, data map[string]any) error {
		if data == nil || data["token"] != "ok" {
			return errAuth
		}
		return nil
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send(`0{"token":"bad"}`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != ConnectError {
		t.Fatalf("got %v", p.Type)
	}
	if msg, _ := p.Data.(map[string]any)["message"].(string); msg != "auth failed" {
		t.Fatalf("message = %q", msg)
	}
}

func TestOnConnectReject(t *testing.T) {
	srv := newTestServer(t)
	srv.OnConnect("/", func(s *Socket) error {
		return errAuth
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	p := tc.recvPacket(5 * time.Second)
	if p.Type != ConnectError {
		t.Fatalf("got %v", p.Type)
	}
}

func TestRoomBroadcast(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "join", func(s *Socket, room string) string {
		s.JoinRoom(room)
		return "ok"
	})
	srv.OnEvent("/", "msg", func(s *Socket, room, text string) {
		s.To(room).Emit("chat", text)
	})
	ts := startTestServer(t, srv)

	clientA := newTestClient(t, ts.URL)
	clientA.send("0")
	clientA.recvPacket(5 * time.Second)
	clientB := newTestClient(t, ts.URL)
	clientB.send("0")
	clientB.recvPacket(5 * time.Second)

	// Each client joins room1 and waits for the server to confirm, so the
	// membership is registered before the broadcast happens.
	for i, tc := range []*testClient{clientA, clientB} {
		tc.send(`2` + string(rune('1'+i)) + `["join","room1"]`)
		p := tc.recvPacket(5 * time.Second)
		if p.Type != Ack || !reflect.DeepEqual(p.Data, []any{"ok"}) {
			t.Fatalf("join ack got %v %#v", p.Type, p.Data)
		}
	}
	clientA.send(`2["msg","room1","hello"]`)

	// B must receive the broadcast; A (the sender) must not.
	p := clientB.recvPacket(5 * time.Second)
	if !reflect.DeepEqual(p.Data, []any{"chat", "hello"}) {
		t.Fatalf("B got %#v", p.Data)
	}
	clientA.expectSilence(300 * time.Millisecond)
}

func TestServerBroadcastToNamespace(t *testing.T) {
	srv := newTestServer(t)
	srv.OnConnect("/", func(s *Socket) error {
		go srv.BroadcastToNamespace("/", "online", s.ID())
		return nil
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	p := tc.recvPacket(5 * time.Second)
	if p.Type != Event || p.Data.([]any)[0] != "online" {
		t.Fatalf("got %v %#v", p.Type, p.Data)
	}
}

func TestBinaryEvent(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "bin", func(s *Socket, name string, data []byte) {
		s.Emit("bin2", name, data)
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	// craft a binary event: 1 attachment
	pkt := &Packet{Type: BinaryEvent, Nsp: "/", ID: -1, Attachments: 1,
		Data: []any{"bin", "blob", placeholder{Placeholder: true, Num: 0}}}
	tc.c.SendMessage(pkt.Encode(), false)
	tc.c.SendMessage([]byte("hello"), true)

	p := tc.recvPacket(5 * time.Second)
	if p.Type != BinaryEvent || p.Attachments != 1 {
		t.Fatalf("got %v attachments=%d", p.Type, p.Attachments)
	}
	data, ok := p.Data.([]any)
	if !ok || data[0] != "bin2" || data[1] != "blob" {
		t.Fatalf("data = %#v", p.Data)
	}
	binary := tc.recv(5 * time.Second)
	if !binary.binary || string(binary.data) != "hello" {
		t.Fatalf("binary = %v %q", binary.binary, binary.data)
	}
}

func TestBinaryEventMultiAttachment(t *testing.T) {
	got := make(chan []byte, 2)
	srv := newTestServer(t)
	srv.OnEvent("/", "bin", func(s *Socket, name string, first []byte, nested map[string]any) {
		got <- first
		if b, ok := nested["deep"].([]byte); ok {
			got <- b
		}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	pkt := &Packet{Type: BinaryEvent, Nsp: "/", ID: -1, Attachments: 2,
		Data: []any{"bin", "x", placeholder{Placeholder: true, Num: 0},
			map[string]any{"deep": placeholder{Placeholder: true, Num: 1}}}}
	tc.c.SendMessage(pkt.Encode(), false)
	tc.c.SendMessage([]byte("one"), true)
	tc.c.SendMessage([]byte("two"), true)

	select {
	case b := <-got:
		if string(b) != "one" {
			t.Fatalf("first = %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("first binary not delivered")
	}
	select {
	case b := <-got:
		if string(b) != "two" {
			t.Fatalf("nested = %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("nested binary not delivered")
	}
}

func TestBinaryAckToServer(t *testing.T) {
	ackCh := make(chan []any, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "start", func(s *Socket) {
		if _, err := s.EmitWithAck("ping?", func(args []any) {
			ackCh <- args
		}); err != nil {
			t.Fatalf("emit with ack: %v", err)
		}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`2["start"]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Event || p.Data.([]any)[0] != "ping?" || p.ID < 0 {
		t.Fatalf("got %v %#v id=%d", p.Type, p.Data, p.ID)
	}

	// reply with a binary ack: 1 attachment
	tc.sendPacket(&Packet{Type: BinaryAck, Nsp: "/", ID: p.ID, Attachments: 1,
		Data: []any{placeholder{Placeholder: true, Num: 0}}})
	tc.c.SendMessage([]byte("pong-binary"), true)

	select {
	case args := <-ackCh:
		if len(args) != 1 {
			t.Fatalf("ack args = %#v", args)
		}
		b, ok := args[0].([]byte)
		if !ok || string(b) != "pong-binary" {
			t.Fatalf("ack binary = %v %q", ok, b)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("ack callback not invoked")
	}
}

func TestBinaryAckToClient(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "ping?", func(s *Socket) []byte {
		return []byte("pong-binary")
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`25["ping?"]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != BinaryAck || p.Attachments != 1 || p.ID != 5 {
		t.Fatalf("got %v attachments=%d id=%d", p.Type, p.Attachments, p.ID)
	}
	binary := tc.recv(5 * time.Second)
	if !binary.binary || string(binary.data) != "pong-binary" {
		t.Fatalf("binary = %v %q", binary.binary, binary.data)
	}
}

func TestBroadcastBinary(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "share", func(s *Socket, data []byte) {
		s.Broadcast("shared", data)
	})
	ts := startTestServer(t, srv)

	tcA := newTestClient(t, ts.URL)
	tcA.send("0")
	tcA.recvPacket(5 * time.Second)
	tcB := newTestClient(t, ts.URL)
	tcB.send("0")
	tcB.recvPacket(5 * time.Second)

	pkt := &Packet{Type: BinaryEvent, Nsp: "/", ID: -1, Attachments: 1,
		Data: []any{"share", placeholder{Placeholder: true, Num: 0}}}
	tcA.c.SendMessage(pkt.Encode(), false)
	tcA.c.SendMessage([]byte("broadcast-me"), true)

	p := tcB.recvPacket(5 * time.Second)
	if p.Type != BinaryEvent || p.Attachments != 1 {
		t.Fatalf("got %v attachments=%d", p.Type, p.Attachments)
	}
	if data, ok := p.Data.([]any); !ok || data[0] != "shared" {
		t.Fatalf("data = %#v", p.Data)
	}
	binary := tcB.recv(5 * time.Second)
	if !binary.binary || string(binary.data) != "broadcast-me" {
		t.Fatalf("binary = %v %q", binary.binary, binary.data)
	}
}

func TestEmitWithAck(t *testing.T) {
	ackCh := make(chan []any, 1)
	srv := newTestServer(t)
	srv.OnEvent("/", "start", func(s *Socket) {
		_, err := s.EmitWithAck("ping?", func(args []any) {
			ackCh <- args
		})
		if err != nil {
			t.Fatalf("emit with ack: %v", err)
		}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`2["start"]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Event || p.Data.([]any)[0] != "ping?" || p.ID < 0 {
		t.Fatalf("got %v %#v id=%d", p.Type, p.Data, p.ID)
	}

	tc.sendPacket(&Packet{Type: Ack, Nsp: "/", ID: p.ID, Data: []any{"pong"}})
	select {
	case args := <-ackCh:
		if len(args) != 1 || args[0] != "pong" {
			t.Fatalf("ack args = %#v", args)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("ack callback not invoked")
	}
}

func TestClientDisconnect(t *testing.T) {
	reasonCh := make(chan string, 1)
	srv := newTestServer(t)
	srv.OnDisconnect("/", func(s *Socket, reason string) {
		reasonCh <- reason
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send("1")
	select {
	case reason := <-reasonCh:
		if reason != "client namespace disconnect" {
			t.Fatalf("reason = %q", reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("disconnect not fired")
	}
}

func TestTransportCloseDisconnect(t *testing.T) {
	reasonCh := make(chan string, 1)
	srv := newTestServer(t)
	srv.OnDisconnect("/", func(s *Socket, reason string) {
		reasonCh <- reason
	})
	ts := startTestServer(t, srv)

	// A websocket close is detected by the server immediately, making the
	// propagation deterministic (a silent polling abandonment is only noticed
	// after the ping timeout).
	tc := newTestClientOpts(t, ts.URL, &engineio.Options{Transports: []string{"websocket"}})
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.c.Close()
	select {
	case reason := <-reasonCh:
		if reason != "transport close" && reason != "transport error" {
			t.Fatalf("reason = %q", reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("disconnect not fired")
	}
}

func TestServerInitiatedDisconnect(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "kick", func(s *Socket) {
		s.Disconnect()
	})
	srv.OnDisconnect("/", func(s *Socket, reason string) {
		if reason != "server namespace disconnect" {
			t.Fatalf("reason = %q", reason)
		}
	})
	ts := startTestServer(t, srv)

	tc := newTestClient(t, ts.URL)
	tc.send("0")
	tc.recvPacket(5 * time.Second)

	tc.send(`2["kick"]`)
	p := tc.recvPacket(5 * time.Second)
	if p.Type != Disconnect {
		t.Fatalf("expected disconnect packet, got %v", p.Type)
	}
}

func TestConcurrentSockets(t *testing.T) {
	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) {
		s.Emit("echoed", msg)
	})
	ts := startTestServer(t, srv)

	const n = 8
	var clients []*testClient
	for i := 0; i < n; i++ {
		c := newTestClient(t, ts.URL)
		c.send("0")
		if p := c.recvPacket(5 * time.Second); p.Type != Connect {
			t.Fatalf("client %d: got %v", i, p.Type)
		}
		clients = append(clients, c)
	}
	for i, c := range clients {
		payload := strings.TrimSpace(`["echo","m` + string(rune('0'+i)) + `"]`)
		c.send("2" + payload)
	}
	for i, c := range clients {
		p := c.recvPacket(5 * time.Second)
		if p.Type != Event {
			t.Fatalf("client %d: got %v", i, p.Type)
		}
	}
}

var errAuth = &authError{}

type authError struct{}

func (e *authError) Error() string { return "auth failed" }
