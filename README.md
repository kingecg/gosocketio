# gosocketio

A pure-Go implementation of the [Socket.IO](https://socket.io) protocol
(Engine.IO v4, Socket.IO v5) with a single dependency:
[`github.com/coder/websocket`](https://github.com/coder/websocket).

It ships both a **server** and a **client**, and interoperates with the
official `socket.io` and `socket.io-client` Node.js packages.

## Features

- **Server and client** in one module, split across two layers:
  - `github.com/kingecg/gosocketio/engineio` — the Engine.IO transport layer
    (polling and websocket, upgrades, ping/pong, session management).
  - `github.com/kingecg/gosocketio/socketio` — the Socket.IO message layer
    (namespaces, events, acknowledgements, binary payloads, reconnection).
- **Namespaces** with per-namespace handlers and connection middleware.
- **Rooms** with room-scoped broadcasts.
- **Acknowledgements** on both the server and the client, with strongly
  typed handler signatures (no boilerplate casting).
- **Binary events** (`BINARY_EVENT` / `BINARY_ACK`) transparently split into
  attachments.
- **Automatic reconnection** on the client with backoff, mirroring
  `socket.io-client` behaviour.
- **Interoperability tested** against the official Node.js server and client.

## Installation

```sh
go get github.com/kingecg/gosocketio
```

Requires Go 1.21+.

## Server

The server is an `http.Handler`. Mount it on any path (the official clients
use `/socket.io/`):

```go
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
	srv.OnEvent("/", "message", func(s *socketio.Socket, text string) {
		srv.BroadcastToNamespace("/", "message", s.ID(), text)
	})
	// Acknowledgement: the return value is sent back to the client.
	srv.OnEvent("/", "ping", func(s *socketio.Socket) string {
		return "pong"
	})

	log.Fatal(http.ListenAndServe(":3000", srv))
}
```

### Namespaces

Register handlers per namespace and connect additional namespaces from the
client:

```go
srv.OnEvent("/admin", "stats", func(s *socketio.Socket) map[string]any {
	return map[string]any{"users": srv.Engine().ClientsCount()}
})
```

The client connects an extra namespace with `ConnectNamespace`:

```go
err := c.ConnectNamespace(ctx, "/admin", map[string]any{"token": "secret"})
```

### Middleware

`Use` runs before a namespace connection is accepted. Return an error to
reject the connection with a `CONNECT_ERROR`:

```go
srv.Use("/admin", func(s *socketio.Socket, data map[string]any) error {
	if data["token"] != "secret" {
		return errors.New("unauthorized")
	}
	return nil
})
```

### Rooms

```go
srv.OnEvent("/", "join", func(s *socketio.Socket, room string) {
	s.JoinRoom(room)
})
srv.OnEvent("/", "room message", func(s *socketio.Socket, room, text string) {
	srv.BroadcastToRoom("/", room, "message", text)
})
```

`Server.BroadcastToRoom` includes the sender. A socket's own `To(room)`
operator targets everyone in the room **except** the sender, matching the
official `socket.to(room)` semantics.

## Client

```go
c, err := socketio.Dial(context.Background(), "http://localhost:3000/socket.io/", nil)
if err != nil {
	log.Fatal(err)
}
defer c.Close()

c.OnEvent("/", "message", func(from, text string) {
	fmt.Printf("%s: %s\n", from, text)
})
c.OnEvent("/", "ping", func() string {
	return "pong"
})

c.Emit("/", "message", "hello")
result, err := c.EmitWithAck("/", "ping", func(args []any) { ... }, "hi")
```

### Options

```go
c, err := socketio.Dial(ctx, url, &socketio.Options{
	Auth: map[string]any{"token": "..."}, // sent with the CONNECT packet
	Reconnection:        true,
	ReconnectionAttempts: 10,
	ReconnectionDelay:    1 * time.Second,
	ReconnectionDelayMax: 5 * time.Second,
	Timeout:              10 * time.Second,
})
```

| Option | Default | Purpose |
| ------ | ------- | ------- |
| `Transports` | `["polling", "websocket"]` | Transports to use, in order. |
| `Auth` | `nil` | Extra data sent with the namespace CONNECT packet. |
| `Reconnection` | `false` | Reconnect automatically after a transport drop. |
| `ReconnectionAttempts` | `0` | Max reconnect attempts (`0` = retry forever). |
| `ReconnectionDelay` | `1s` | Initial backoff between attempts. |
| `ReconnectionDelayMax` | `5s` | Maximum backoff. |
| `RandomizationFactor` | `0.5` | Random delay jitter. |
| `Timeout` | `10s` | Timeout for the initial connection. |

### Handler signatures

Handlers are called through reflection, so argument types are checked and
decoded for you. Parameters after `*Socket` map to event arguments; return
values become the acknowledgement payload.

- `func(s *Socket)`
- `func(s *Socket, text string, count int, meta map[string]any)`
- `func(s *Socket, payload *T)`

The same applies to client handlers without the `*Socket` parameter. Non-nil
`error` return values are sent as their message string.

## Binary payloads

Any value passed through an event or an acknowledgement is encoded as JSON.
`[]byte` values are sent as binary attachments:

```go
c.OnEvent("/", "download", func() []byte {
	return []byte("file contents")
})
srv.OnEvent("/", "download", func(s *socketio.Socket) []byte {
	return []byte("file contents")
})
```

## Interoperability

`socketio/interop_test.go` runs both directions against the official Node.js
implementation:

- **Go client → Node server**: events, binary events, acknowledgements,
  namespaces, connection errors and transport upgrades.
- **Node client → Go server**: the same surface in reverse, driven by
  `interop/js-client/test.mjs`.

Run them with the Go tests (they locate the Node server automatically):

```sh
go test ./socketio/ -run Interop -v
```

## Examples

See `examples/` for a runnable chat server and client:

```sh
go run ./examples/chat-server
go run ./examples/chat-client
```

## Development

```sh
go test ./... -race
```

Tests exercise real Engine.IO polling and websocket sessions, transport
upgrades, and race-free concurrent broadcast and reconnection paths.
