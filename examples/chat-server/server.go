package main

import (
	"log"
	"net/http"

	"github.com/kingecg/gosocketio/socketio"
)

func main() {
	// NewServerWithConfig sets a mount path guard and CORS alongside the
	// default engine options. AllowAll is handy for local development.
	srv := socketio.NewServerWithConfig(&socketio.ServerConfig{
		Path: "/socket.io",
		CORS: socketio.AllowAll(),
	})

	// RegisterNamespace pre-registers a namespace so it exists up front
	// (a no-op if already present). The root "/" is implicit and cannot be
	// re-registered; here we pre-register a secondary "/lobby" namespace.
	if err := srv.RegisterNamespace("/lobby"); err != nil {
		log.Fatal(err)
	}

	// /lobby is a secondary namespace the client connects explicitly. Its
	// broadcasts are namespace-scoped and reach only that namespace.
	srv.OnEvent("/lobby", "message", func(s *socketio.Socket, text string) {
		log.Printf("[lobby] %s: %s", s.ID(), text)
		srv.BroadcastToNamespace("/lobby", "message", s.ID(), text)
	})

	// OnAny is a catch-all fired before the named handler for every event.
	srv.OnAny("/", func(s *socketio.Socket, event string, args []any) {
		log.Printf("%s -> %s %v", s.ID(), event, args)
	})

	// OnError receives handler dispatch failures (bad signatures, wrong
	// argument types) instead of silently dropping them.
	srv.OnError("/", func(s *socketio.Socket, err error) {
		log.Printf("%s handler error: %v", s.ID(), err)
	})

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

	// ToExcept broadcasts to a room while excluding the sender and specific
	// ids. The client uses it to whisper to everyone in a room but one peer.
	srv.OnEvent("/", "except", func(s *socketio.Socket, room, exceptID, text string) {
		s.ToExcept(room, []string{exceptID}).Emit("room message", s.ID(), text)
	})

	// Acknowledgement round-trip: the client's callback receives "pong".
	srv.OnEvent("/", "ping", func(s *socketio.Socket) string {
		return "pong"
	})

	log.Println("chat server listening on :3000 (path /socket.io, CORS allow-all)")
	if err := http.ListenAndServe(":3000", srv); err != nil {
		log.Fatal(err)
	}
}
