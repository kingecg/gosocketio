# Official JS client interop test

`test.mjs` drives the gosocketio server with the official `socket.io-client`
(v4) and verifies text/binary events, acknowledgements in both directions,
custom namespaces, connect errors and server-initiated disconnects.

It is invoked automatically by `TestOfficialJSClientInterop` in
`socketio/interop_test.go`, which skips the test when `node` or the dependency
is unavailable.

## Setup

```sh
npm install
```

## Run manually

```sh
# against a server listening on port 3000
node test.mjs 3000
```

Exit code 0 and a final `ALL PASS` line indicate success.
