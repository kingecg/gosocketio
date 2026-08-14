// Package engineio implements the Engine.IO v4 protocol layer: session
// management, HTTP long-polling and WebSocket transports, transport upgrades
// and the heartbeat.
//
// A Server is an http.Handler serving Engine.IO sessions, and a Client dials
// a remote Engine.IO endpoint; both deliver raw message packets. The Socket.IO
// protocol (github.com/kingecg/gosocketio/socketio) is layered on top of this
// package.
//
// Protocol reference: https://socket.io/docs/v4/engine-io-protocol/
package engineio
