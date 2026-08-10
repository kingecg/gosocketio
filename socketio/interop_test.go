package socketio

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestOfficialJSClientInterop verifies the server against the official
// socket.io-client (v4). It is skipped when node or the client dependency is
// not available (see interop/js-client/README.md).
func TestOfficialJSClientInterop(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	root, err := filepath.Abs(filepath.Join("..", "interop", "js-client"))
	if err != nil {
		t.Fatalf("resolve interop dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "node_modules", "socket.io-client")); err != nil {
		t.Skip("socket.io-client not installed; run `npm install` in interop/js-client")
	}

	srv := newTestServer(t)
	srv.OnEvent("/", "echo", func(s *Socket, msg string) string {
		return "ack:" + msg
	})
	srv.OnEvent("/", "binget", func(s *Socket, data []byte) []byte {
		return append([]byte("pong:"), data...)
	})
	srv.OnEvent("/", "binpush?", func(s *Socket) {
		s.Emit("binpush", []byte("hi-binary"))
	})
	// The handler performs a round-trip: it asks the client for a binary ack
	// and blocks until the reply arrives. Because handlers run off the packet
	// processing goroutine, this cannot deadlock even over websocket.
	srv.OnEvent("/", "needbinack?", func(s *Socket) string {
		resultCh := make(chan string, 1)
		if _, err := s.EmitWithAck("needbinack", func(args []any) {
			if len(args) == 1 {
				if b, ok := args[0].([]byte); ok {
					resultCh <- "ok:" + string(b)
					return
				}
			}
			resultCh <- "ok:wrong"
		}, "trigger"); err != nil {
			return "ok:disconnected"
		}
		select {
		case r := <-resultCh:
			return r
		case <-time.After(5 * time.Second):
			return "ok:timeout"
		}
	})
	srv.OnEvent("/admin", "adminping", func(s *Socket) string {
		return "admin:pong"
	})
	srv.OnEvent("/", "kick", func(s *Socket) {
		s.Disconnect()
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() { _ = http.Serve(ln, srv) }()
	port := ln.Addr().(*net.TCPAddr).Port

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "test.mjs", fmt.Sprint(port))
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	t.Logf("node output:\n%s", out)
	if err != nil {
		t.Fatalf("interop test failed: %v", err)
	}
}
