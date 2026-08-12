# Official JS server interop test

`server.mjs` runs the official `socket.io` (v4) server. It is driven by the
gosocketio Go client, which verifies text/binary events, acknowledgements in
both directions, custom namespaces, connect errors and server-initiated
disconnects against the reference implementation.

It is invoked automatically by `TestOfficialJSServerInterop` in
`socketio/interop_test.go`, which skips the test when `node` or the dependency
is unavailable.

## Setup

```sh
npm install
```

## Run manually

```sh
# prints "PORT <n>" when ready, then waits for a Go client
node server.mjs
```
