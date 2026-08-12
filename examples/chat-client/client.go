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
			case strings.HasPrefix(line, "/join "):
				_ = c.Emit("/", "join", strings.TrimPrefix(line, "/join "))
			case strings.HasPrefix(line, "/leave "):
				_ = c.Emit("/", "leave", strings.TrimPrefix(line, "/leave "))
			case strings.HasPrefix(line, "/room "):
				parts := strings.SplitN(strings.TrimPrefix(line, "/room "), " ", 2)
				if len(parts) == 2 {
					_ = c.Emit("/", "room message", parts[0], parts[1])
				}
			case line != "":
				_ = c.Emit("/", "message", line)
			}
		}
	}()

	select {}
}
