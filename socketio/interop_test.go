package socketio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		case <-time.After(testWait):
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

// TestOfficialJSServerInterop verifies the client against the official
// socket.io (v4) server. The Go client drives text/binary events,
// acknowledgements in both directions, a custom namespace, a rejected
// namespace and a server-initiated disconnect against the reference
// implementation, including the polling->websocket transport upgrade. It is
// skipped when node or the server dependency is not available (see
// interop/js-server/README.md).
func TestOfficialJSServerInterop(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	root, err := filepath.Abs(filepath.Join("..", "interop", "js-server"))
	if err != nil {
		t.Fatalf("resolve interop dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "node_modules", "socket.io")); err != nil {
		t.Skip("socket.io not installed; run `npm install` in interop/js-server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "server.mjs")
	cmd.Dir = root
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	go io.Copy(os.Stderr, stderr)

	portCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			t.Logf("node: %s", line)
			if rest, ok := strings.CutPrefix(line, "PORT "); ok {
				portCh <- rest
				return
			}
		}
		portCh <- ""
	}()
	var port string
	select {
	case port = <-portCh:
	case <-ctx.Done():
		t.Fatalf("server did not report a port: %v", ctx.Err())
	}
	if port == "" {
		t.Fatalf("server did not report a port")
	}

	url := "http://127.0.0.1:" + port + "/socket.io/"
	c, err := Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if !c.Connected("/") {
		t.Error("root namespace not connected")
	}
	if c.ID("/") == "" {
		t.Error("root namespace has no sid")
	}

	ackIn := func(name string, done <-chan []any) []any {
		t.Helper()
		select {
		case args := <-done:
			return args
		case <-time.After(testWait):
			t.Fatalf("%s ack timed out", name)
			return nil
		}
	}

	{
		ack := make(chan []any, 1)
		if _, err := c.EmitWithAck("/", "echo", func(args []any) { ack <- args }, "hello"); err != nil {
			t.Fatalf("echo emit: %v", err)
		}
		if args := ackIn("echo", ack); len(args) != 1 || args[0] != "ack:hello" {
			t.Errorf("echo ack = %v, want [ack:hello]", args)
		}
	}

	{
		ack := make(chan []any, 1)
		if _, err := c.EmitWithAck("/", "binget", func(args []any) { ack <- args }, []byte("binary-data")); err != nil {
			t.Fatalf("binget emit: %v", err)
		}
		args := ackIn("binget", ack)
		if len(args) != 1 {
			t.Fatalf("binget ack = %v, want 1 arg", args)
		}
		b, ok := args[0].([]byte)
		if !ok || string(b) != "pong:binary-data" {
			t.Errorf("binget ack = %v (%T), want []byte(\"pong:binary-data\")", args[0], args[0])
		}
	}

	{
		got := make(chan []byte, 1)
		c.OnEvent("/", "binpush", func(b []byte) { got <- b })
		if err := c.Emit("/", "binpush?"); err != nil {
			t.Fatalf("binpush? emit: %v", err)
		}
		select {
		case b := <-got:
			if string(b) != "hi-binary" {
				t.Errorf("binpush payload = %q, want %q", b, "hi-binary")
			}
		case <-time.After(testWait):
			t.Fatal("binpush timed out")
		}
	}

	{
		trigger := make(chan string, 1)
		c.OnEvent("/", "needbinack", func(payload string) []byte {
			trigger <- payload
			return []byte("client-binary")
		})
		ack := make(chan []any, 1)
		if _, err := c.EmitWithAck("/", "needbinack?", func(args []any) { ack <- args }); err != nil {
			t.Fatalf("needbinack? emit: %v", err)
		}
		if args := ackIn("needbinack", ack); len(args) != 1 || args[0] != "ok:client-binary" {
			t.Errorf("needbinack ack = %v, want [ok:client-binary]", args)
		}
		select {
		case p := <-trigger:
			if p != "trigger" {
				t.Errorf("needbinack payload = %q, want %q", p, "trigger")
			}
		case <-time.After(testWait):
			t.Error("needbinack handler not invoked")
		}
	}

	{
		nctx, ncancel := context.WithTimeout(ctx, testWait)
		defer ncancel()
		if err := c.ConnectNamespace(nctx, "/admin", nil); err != nil {
			t.Fatalf("connect /admin: %v", err)
		}
		if !c.Connected("/admin") {
			t.Error("/admin not connected")
		}
		ack := make(chan []any, 1)
		if _, err := c.EmitWithAck("/admin", "adminping", func(args []any) { ack <- args }); err != nil {
			t.Fatalf("adminping emit: %v", err)
		}
		if args := ackIn("adminping", ack); len(args) != 1 || args[0] != "admin:pong" {
			t.Errorf("adminping ack = %v, want [admin:pong]", args)
		}
	}

	{
		errCh := make(chan error, 1)
		c.OnConnectError("/nonexistent", func(err error) { errCh <- err })
		nctx, ncancel := context.WithTimeout(ctx, testWait)
		defer ncancel()
		err := c.ConnectNamespace(nctx, "/nonexistent", nil)
		if err == nil {
			t.Error("connecting to /nonexistent succeeded, want error")
		} else {
			t.Logf("connect /nonexistent error: %v", err)
		}
		select {
		case e := <-errCh:
			if e == nil {
				t.Error("OnConnectError received nil")
			}
		case <-time.After(testWait):
			t.Error("OnConnectError not fired")
		}
		if c.Connected("/nonexistent") {
			t.Error("/nonexistent marked connected")
		}
	}

	{
		disc := make(chan string, 1)
		c.OnDisconnect("/", func(reason string) { disc <- reason })
		if err := c.Emit("/", "kick"); err != nil {
			t.Fatalf("kick emit: %v", err)
		}
		select {
		case reason := <-disc:
			if reason != "io server disconnect" {
				t.Errorf("disconnect reason = %q, want %q", reason, "io server disconnect")
			}
		case <-time.After(testWait):
			t.Fatal("server-initiated disconnect not observed")
		}
	}
}
