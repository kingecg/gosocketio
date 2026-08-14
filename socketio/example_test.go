package socketio_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"sort"
	"time"

	"github.com/kingecg/gosocketio/socketio"
)

// ExampleDial demonstrates a full Socket.IO round trip: start a server,
// connect a client, emit an event and receive the acknowledgement. The
// handler's return value becomes the ack payload.
func ExampleDial() {
	srv := socketio.NewServer(nil)
	srv.OnEvent("/", "ping", func(s *socketio.Socket) string {
		return "pong"
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	c, err := socketio.Dial(context.Background(), httpSrv.URL+"/socket.io/", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ack := make(chan []any, 1)
	if _, err := c.EmitWithAck("/", "ping", func(args []any) {
		ack <- args
	}); err != nil {
		log.Fatal(err)
	}
	select {
	case args := <-ack:
		fmt.Printf("ack: %v\n", args)
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for ack")
	}
	// Output:
	// ack: [pong]
}

// ExampleServer_OnEvent demonstrates a server-side event handler receiving
// typed arguments. []byte arguments travel as binary attachments and are
// reconstructed before the handler is invoked.
func ExampleServer_OnEvent() {
	srv := socketio.NewServer(nil)
	srv.OnEvent("/", "download", func(s *socketio.Socket, name string, data []byte) {
		fmt.Printf("server received %q (%d bytes)\n", name, len(data))
		s.Emit("file", name, data)
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	c, err := socketio.Dial(context.Background(), httpSrv.URL+"/socket.io/", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	got := make(chan struct{})
	c.OnEvent("/", "file", func(name string, data []byte) {
		fmt.Printf("client received %q (%d bytes)\n", name, len(data))
		close(got)
	})
	if err := c.Emit("/", "download", "logo.png", []byte{0x89, 0x50, 0x4e, 0x47}); err != nil {
		log.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for binary echo")
	}
	// Output:
	// server received "logo.png" (4 bytes)
	// client received "logo.png" (4 bytes)
}

// ExampleServer_Use demonstrates a connection middleware that validates the
// CONNECT payload (for example an auth token) and rejects unauthorized
// clients with a CONNECT_ERROR.
func ExampleServer_Use() {
	srv := socketio.NewServer(nil)
	srv.Use("/", func(s *socketio.Socket, data map[string]any) error {
		if token, _ := data["token"].(string); token != "secret" {
			return errors.New("unauthorized")
		}
		return nil
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	good, err := socketio.Dial(context.Background(), httpSrv.URL+"/socket.io/", &socketio.Options{
		Auth: map[string]any{"token": "secret"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("authorized: connected")
	good.Close()

	if _, err := socketio.Dial(context.Background(), httpSrv.URL+"/socket.io/", &socketio.Options{
		Auth: map[string]any{"token": "wrong"},
	}); err != nil {
		fmt.Println("unauthorized:", err)
	}
	// Output:
	// authorized: connected
	// unauthorized: unauthorized
}

// ExampleServer_BroadcastToRoom demonstrates room-scoped broadcasting: every
// socket that joined the room receives the message (including the sender),
// while sockets outside the room do not.
func ExampleServer_BroadcastToRoom() {
	srv := socketio.NewServer(nil)
	srv.OnEvent("/", "join", func(s *socketio.Socket, room string) string {
		s.JoinRoom(room)
		return "joined"
	})
	srv.OnEvent("/", "room message", func(s *socketio.Socket, text string) {
		srv.BroadcastToRoom("/", "lobby", "room message", text)
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	connect := func() *socketio.Client {
		c, err := socketio.Dial(context.Background(), httpSrv.URL+"/socket.io/", nil)
		if err != nil {
			log.Fatal(err)
		}
		return c
	}
	alice := connect()
	defer alice.Close()
	bob := connect()
	defer bob.Close()

	// The server acknowledges each join, so membership is guaranteed before
	// the broadcast happens.
	join := func(c *socketio.Client) {
		done := make(chan []any, 1)
		if _, err := c.EmitWithAck("/", "join", func(args []any) { done <- args }, "lobby"); err != nil {
			log.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			log.Fatal("timed out waiting for join ack")
		}
	}
	join(alice)
	join(bob)

	// Carol connects but never joins the room.
	carol := connect()
	defer carol.Close()
	carolGot := make(chan struct{})
	carol.OnEvent("/", "room message", func(text string) {
		close(carolGot)
	})

	got := make(chan string, 2)
	alice.OnEvent("/", "room message", func(text string) { got <- "alice: " + text })
	bob.OnEvent("/", "room message", func(text string) { got <- "bob: " + text })

	if err := alice.Emit("/", "room message", "hello lobby"); err != nil {
		log.Fatal(err)
	}
	msgs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case m := <-got:
			msgs = append(msgs, m)
		case <-time.After(5 * time.Second):
			log.Fatal("timed out waiting for room broadcast")
		}
	}
	// Sort ensures deterministic output regardless of receive order.
	sort.Strings(msgs)
	for _, m := range msgs {
		fmt.Println(m)
	}
	select {
	case <-carolGot:
		log.Fatal("carol received a message despite not being in the room")
	default:
		fmt.Println("carol: no message (not in the room)")
	}
	// Output:
	// alice: hello lobby
	// bob: hello lobby
	// carol: no message (not in the room)
}
