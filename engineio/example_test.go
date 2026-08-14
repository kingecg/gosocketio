package engineio_test

import (
	"context"
	"fmt"
	"log"
	"net/http/httptest"
	"time"

	"github.com/kingecg/gosocketio/engineio"
)

// ExampleDial demonstrates a full Engine.IO round trip: dial a server,
// send a message and receive the echo. By default the client starts on HTTP
// long-polling and transparently upgrades to WebSocket.
func ExampleDial() {
	srv := engineio.NewServer(nil)
	srv.OnData(func(s *engineio.Socket, data []byte, binary bool) {
		s.SendMessage(data, binary)
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	c, err := engineio.Dial(context.Background(), httpSrv.URL+"/socket.io/", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	echo := make(chan string, 1)
	c.OnData(func(_ *engineio.Client, data []byte, _ bool) {
		echo <- string(data)
	})
	if err := c.SendMessage([]byte("hello"), false); err != nil {
		log.Fatal(err)
	}
	select {
	case msg := <-echo:
		fmt.Println("echo:", msg)
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for echo")
	}
	// Output:
	// echo: hello
}

// ExampleServer demonstrates wiring the server-side handlers: OnConnect,
// OnData and OnClose. The server is an http.Handler, so it can be mounted on
// any path of an existing HTTP server.
func ExampleServer() {
	disconnected := make(chan struct{})
	srv := engineio.NewServer(nil)
	srv.OnConnect(func(s *engineio.Socket) {
		fmt.Println("client connected")
	})
	srv.OnData(func(s *engineio.Socket, data []byte, binary bool) {
		s.SendMessage(data, binary)
	})
	srv.OnClose(func(s *engineio.Socket, reason string, err error) {
		close(disconnected)
	})
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	c, err := engineio.Dial(context.Background(), httpSrv.URL+"/socket.io/", nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Close(); err != nil {
		log.Fatal(err)
	}
	select {
	case <-disconnected:
		fmt.Println("client disconnected")
	case <-time.After(5 * time.Second):
		log.Fatal("server never observed the disconnect")
	}
	// Output:
	// client connected
	// client disconnected
}
