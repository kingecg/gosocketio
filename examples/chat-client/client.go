package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kingecg/gosocketio/socketio"
)

func main() {
	c, err := socketio.Dial(context.Background(), "http://localhost:3000/socket.io/", &socketio.Options{
		Reconnection: true,
	})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer c.Close()

	c.OnConnect("/", func() {
		fmt.Println("connected")
	})
	c.OnDisconnect("/", func(reason string) {
		fmt.Println("disconnected:", reason)
	})
	c.OnEvent("/", "message", func(from, text string) {
		fmt.Printf("%s: %s\n", from, text)
	})
	c.OnEvent("/", "room message", func(from, text string) {
		fmt.Printf("[room] %s: %s\n", from, text)
	})
	c.OnError("/", func(err error) {
		fmt.Printf("handler error: %v\n", err)
	})

	// Connect an explicitly pre-registered secondary namespace (server-side
	// RegisterNamespace). It has its own message event below.
	if err := c.ConnectNamespace(context.Background(), "/lobby", nil); err != nil {
		log.Printf("connect lobby: %v", err)
	}
	c.OnEvent("/lobby", "message", func(from, text string) {
		fmt.Printf("[lobby] %s: %s\n", from, text)
	})

	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			switch {
			case line == "/ping":
				if _, err := c.EmitWithAck("/", "ping", func(args []any) {
					fmt.Printf("ack: %v\n", args)
				}); err != nil {
					log.Printf("ping: %v", err)
				}
			case strings.HasPrefix(line, "/lobby "):
				_ = c.Emit("/lobby", "message", strings.TrimPrefix(line, "/lobby "))
			case strings.HasPrefix(line, "/join "):
				_ = c.Emit("/", "join", strings.TrimPrefix(line, "/join "))
			case strings.HasPrefix(line, "/leave "):
				_ = c.Emit("/", "leave", strings.TrimPrefix(line, "/leave "))
			case strings.HasPrefix(line, "/room "):
				parts := strings.SplitN(strings.TrimPrefix(line, "/room "), " ", 2)
				if len(parts) == 2 {
					_ = c.Emit("/", "room message", parts[0], parts[1])
				}
			case strings.HasPrefix(line, "/except "):
				// /except <room> <exceptID> <text> — whisper to the room but
				// exclude the given peer id.
				parts := strings.SplitN(strings.TrimPrefix(line, "/except "), " ", 3)
				if len(parts) == 3 {
					_ = c.Emit("/", "except", parts[0], parts[1], parts[2])
				}
			case line != "":
				_ = c.Emit("/", "message", line)
			}
		}
	}()

	select {}
}
