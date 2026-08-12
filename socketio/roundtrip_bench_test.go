package socketio

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkRoundTrip measures end-to-end latency of an event with an
// acknowledgement between a real client and server over the polling
// transport, upgraded to websocket.
func BenchmarkRoundTrip(b *testing.B) {
	srv := NewServer(nil)
	srv.OnEvent("/", "echo", func(s *Socket, payload string) string {
		return payload
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c, err := Dial(context.Background(), ts.URL+"/socket.io/", &Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := make(chan struct{})
		payload := fmt.Sprintf("msg-%d", seq.Add(1))
		if _, err := c.EmitWithAck("/", "echo", func(args []any) { close(ch) }, payload); err != nil {
			b.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			b.Fatal("ack timeout")
		}
	}
}

// BenchmarkRoundTripParallel measures throughput with concurrent clients on
// the same connection, exercising ordered packet processing.
func BenchmarkRoundTripParallel(b *testing.B) {
	srv := NewServer(nil)
	srv.OnEvent("/", "echo", func(s *Socket, payload string) string {
		return payload
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c, err := Dial(context.Background(), ts.URL+"/socket.io/", &Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan struct{})
			var once sync.Once
			payload := fmt.Sprintf("msg-%d", seq.Add(1))
			if _, err := c.EmitWithAck("/", "echo", func(args []any) {
				once.Do(func() { close(ch) })
			}, payload); err != nil {
				b.Fatal(err)
			}
			select {
			case <-ch:
			case <-time.After(10 * time.Second):
				b.Fatal("ack timeout")
			}
		}
	})
}

// BenchmarkBroadcast measures fan-out to a single client receiving a
// namespace broadcast.
func BenchmarkBroadcast(b *testing.B) {
	srv := NewServer(nil)
	srv.OnEvent("/", "trigger", func(s *Socket) {})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c, err := Dial(context.Background(), ts.URL+"/socket.io/", &Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	got := make(chan struct{}, 16)
	c.OnEvent("/", "fanout", func(from string) { got <- struct{}{} })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.BroadcastToNamespace("/", "fanout", "srv")
		select {
		case <-got:
		case <-time.After(10 * time.Second):
			b.Fatal("broadcast timeout")
		}
	}
}
