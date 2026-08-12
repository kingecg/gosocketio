package main

import (
	"log"
	"net/http"

	"github.com/kingecg/gosocketio/socketio"
)

func main() {
	srv := socketio.NewServer(nil)

	srv.OnConnect("/", func(s *socketio.Socket) error {
		log.Printf("%s connected", s.ID())
		return nil
	})
	srv.OnDisconnect("/", func(s *socketio.Socket, reason string) {
		log.Printf("%s disconnected: %s", s.ID(), reason)
	})

	// Broadcast every chat message to the whole namespace.
	srv.OnEvent("/", "message", func(s *socketio.Socket, text string) {
		log.Printf("%s: %s", s.ID(), text)
		srv.BroadcastToNamespace("/", "message", s.ID(), text)
	})

	// Rooms: join/leave plus a room-scoped broadcast that includes the sender.
	srv.OnEvent("/", "join", func(s *socketio.Socket, room string) {
		s.JoinRoom(room)
	})
	srv.OnEvent("/", "leave", func(s *socketio.Socket, room string) {
		s.LeaveRoom(room)
	})
	srv.OnEvent("/", "room message", func(s *socketio.Socket, room, text string) {
		srv.BroadcastToRoom("/", room, "room message", s.ID(), text)
	})

	// Acknowledgement round-trip: the client's callback receives "pong".
	srv.OnEvent("/", "ping", func(s *socketio.Socket) string {
		return "pong"
	})

	log.Println("chat server listening on :3000")
	if err := http.ListenAndServe(":3000", srv); err != nil {
		log.Fatal(err)
	}
}
