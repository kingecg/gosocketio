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
- **Namespaces** with per-namespace handlers, connection middleware and
  explicit pre-registration (`RegisterNamespace`).
- **Rooms** with room-scoped broadcasts, per-socket exclusion
  (`ToExcept`/`Except`) and a **pluggable adapter** (in-memory by default,
  bring your own, e.g. Redis).
- **Acknowledgements** on both the server and the client, with strongly
  typed handler signatures (no boilerplate casting).
- **Binary events** (`BINARY_EVENT` / `BINARY_ACK`) transparently split into
  attachments; `[]byte` and `io.Reader` payloads accepted on emit.
- **Automatic reconnection** on the client with backoff (per-namespace auth
  preserved across reconnects), mirroring `socket.io-client` behaviour.
- **Server configuration** via `NewServerWithConfig`: mount path guard and
  CORS (allow-all or explicit origin allow-list, polling + websocket).
- **Error hooks** (`OnError` on server and client) and a **catch-all
  `OnAny`** event hook on the server.
- **Typed error sentinels** matchable with `errors.Is` (`ErrInvalidPacket`,
  `ErrHeartbeatTimeout`, `ErrPayloadTooLarge`, `ErrNamespaceNotConnected`,
  `ErrHandlerMismatch`).
- **maxPayload truncation** drops overflow packets past the configured
  buffer limit instead of killing the connection.
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

Namespaces are created lazily on first use. To register one explicitly
ahead of time (e.g. so a factory-applied adapter is assigned to it):

```go
err := srv.RegisterNamespace("/admin")
```

`RegisterNamespace` returns an error for a malformed name (missing leading
`/` or empty); registering the same namespace twice is a no-op.

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

### Server configuration

`NewServer` takes only engine options. Use `NewServerWithConfig` to also set
a mount path guard and CORS rules:

```go
srv := socketio.NewServerWithConfig(&socketio.ServerConfig{
	Engine: opts,                       // engineio.Options (nil = defaults)
	Path:   "/socket.io",               // serve only under this prefix ("" = no guard)
	CORS:   socketio.AllowAll(),        // or &CORSConfig{AllowedOrigins: []string{"https://app.example.com"}}
})
```

- `Path` guards the handler: requests outside the prefix get `404`.
- `CORS` applies when set: preflight `OPTIONS` is answered with the proper
  headers, disallowed origins get `403`, and websocket upgrades check the
  `Origin` header too.
- `NewServer(opts)` is exactly `NewServerWithConfig(&ServerConfig{Engine: opts})`
  and adds no path guard or CORS.

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

To broadcast to a room while excluding specific sockets, use `ToExcept`:

```go
// Server-level: exclude ids A and B from the room broadcast.
srv.ToExcept("/", "room", []string{idA, idB}, "message", "hello")

// Socket-level: excludes the sender plus any extra ids, like socket.to(room).except(ids).
s.ToExcept("room", []string{idA}).Emit("message", "hello")

// Or chain on the room operator directly.
s.To("room").Except("idA").Emit("message", "hello")
```

`ToExcept` with an empty exclusion list behaves like `To(room)` /
`BroadcastToRoom`.

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
`[]byte` values are sent as binary attachments. `io.Reader` values are also
accepted and read to memory at emit time:

```go
c.OnEvent("/", "download", func() []byte {
	return []byte("file contents")
})
srv.OnEvent("/", "download", func(s *socketio.Socket) []byte {
	return []byte("file contents")
})

// Emitting an io.Reader is equivalent to emitting its []byte contents.
srv.OnEvent("/", "upload", func(s *socketio.Socket, f *os.File) {
	c.Emit("/", "upload", f)
})
```

An `io.Reader` that fails to read is dropped with a warning instead of
corrupting the connection.

## Error handling

The package exposes typed sentinel errors that work with `errors.Is`:

```go
var (
	// engineio layer
	ErrInvalidPacket      // transport packet could not be decoded
	ErrHeartbeatTimeout   // peer stopped answering pings
	ErrPayloadTooLarge    // single polling packet exceeds maxPayload

	// socketio layer
	ErrNamespaceNotConnected // emit on a namespace the client never connected (wraps ErrNotConnected)
	ErrHandlerMismatch       // handler signature does not match the event args
)
```

`OnError` hooks receive dispatch failures (bad handler signatures, wrong
argument types) instead of silently dropping them:

```go
// Server: the *Socket of the sender is provided.
srv.OnError("/", func(s *socketio.Socket, err error) {
	log.Printf("%s handler error: %v", s.ID(), err)
})

// Client: same hook without the socket.
c.OnError("/", func(err error) {
	log.Printf("handler error: %v", err)
})
```

`OnAny` is a catch-all hook fired for every event (before the named handler):

```go
srv.OnAny("/", func(s *socketio.Socket, event string, args []any) {
	log.Printf("%s -> %s %v", s.ID(), event, args)
})
```

## Adapters

Rooms and socket membership are backed by a pluggable `Adapter`. The default
`NewMemoryAdapter` is a single-node in-memory implementation. Provide your
own (e.g. backed by Redis) via `ServerConfig.Adapter` or `SetAdapterFactory`:

```go
type Adapter interface {
	AddSocket(id string)
	RemoveSocket(id string)
	AddToRoom(room, id string)
	RemoveFromRoom(room, id string)
	Broadcast(room string, except []string, deliver func(id string))
	Sockets() []string
	SocketsCount() int
	Close() error
}

type AdapterFactory func(nsp string) Adapter

srv := socketio.NewServerWithConfig(&socketio.ServerConfig{
	Adapter: func(nsp string) socketio.Adapter {
		return myRedisAdapter(nsp)
	},
})
// Or later — applies only to namespaces created after the call:
srv.SetAdapterFactory(func(nsp string) socketio.Adapter {
	return myRedisAdapter(nsp)
})
```

`Server.Close()` calls `Close()` on every namespace's adapter.

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
