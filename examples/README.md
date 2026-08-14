# Chat example

A chat server and client demonstrating the `socketio` package: broadcast
events, rooms, acknowledgements, and the newer additions — CORS + path
config (`NewServerWithConfig`), `OnAny` / `OnError` hooks, `ToExcept`
exclusions, and `RegisterNamespace`.

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
- `/except <room> <exceptID> <text>` — send to a room but exclude a peer
  (via `Socket.ToExcept`)
- `/lobby <text>` — send a message on the explicitly pre-registered
  `/lobby` namespace (server-side `RegisterNamespace`, client connects it
  with `ConnectNamespace`)
- `/ping` — exercise an acknowledgement round-trip

The server is started with `NewServerWithConfig` (mount path `/socket.io`,
CORS allow-all), pre-registers the `/` namespace with `RegisterNamespace`,
and installs an `OnAny` catch-all plus an `OnError` handler-dispatch hook.

Note that `Server.BroadcastToRoom` includes the sender, while a socket's own
`To(room)` operator targets everyone in the room except the sender, matching
the official socket.io `socket.to(room)` semantics.

Stop the server to watch the client reconnect (enabled via `Reconnection`).
