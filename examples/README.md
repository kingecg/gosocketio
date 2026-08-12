# Chat example

A minimal chat server and client demonstrating the `socketio` package:
broadcast events, rooms, and acknowledgements.

## Run

Start the server:

```sh
go run ./examples/chat-server
```

In another terminal, start one or more clients:

```sh
go run ./examples/chat-client
```

## Commands

In the client, type a plain line to broadcast a message to everyone, or use:

- `/join <room>` / `/leave <room>` — join or leave a room
- `/room <room> <text>` — send a message to a room only
- `/ping` — exercise an acknowledgement round-trip

Note that `Server.BroadcastToRoom` includes the sender, while a socket's own
`To(room)` operator targets everyone in the room except the sender, matching
the official socket.io `socket.to(room)` semantics.

Stop the server to watch the client reconnect (enabled via `Reconnection`).
