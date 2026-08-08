package engineio

import (
	"context"
	"net/http"
	"time"
)

// Protocol revision supported by this package.
const Protocol = 4

// Options configures an Engine.IO server or client session.
type Options struct {
	// PingInterval is how long to wait before sending a new ping packet.
	// Default: 25s.
	PingInterval time.Duration

	// PingTimeout is how long to wait for a pong packet before considering
	// the connection closed. Default: 20s.
	PingTimeout time.Duration

	// UpgradeTimeout is how long to wait for a client to complete a
	// transport upgrade before cancelling it. Default: 10s.
	UpgradeTimeout time.Duration

	// MaxHTTPBufferSize is the maximum size in bytes of a received payload
	// or POST body before closing the session. Default: 1 MiB.
	MaxHTTPBufferSize int64

	// Transports lists the enabled transports. Default: ["polling", "websocket"].
	Transports []string

	// AllowUpgrades enables or disables transport upgrades. Default: true.
	AllowUpgrades bool

	// AllowRequest is an optional authorization hook invoked on handshake
	// requests. Return an error to reject the request with HTTP 403.
	AllowRequest func(r *http.Request) error

	// GenerateID overrides the session id generator. Default: random base64.
	GenerateID func(r *http.Request) string
}

func (o *Options) withDefaults() Options {
	opts := *o
	if opts.PingInterval == 0 {
		opts.PingInterval = 25 * time.Second
	}
	if opts.PingTimeout == 0 {
		opts.PingTimeout = 20 * time.Second
	}
	if opts.UpgradeTimeout == 0 {
		opts.UpgradeTimeout = 10 * time.Second
	}
	if opts.MaxHTTPBufferSize == 0 {
		opts.MaxHTTPBufferSize = 1 << 20
	}
	if len(opts.Transports) == 0 {
		opts.Transports = []string{"polling", "websocket"}
	}
	// Upgrades default to enabled when the websocket transport is available.
	opts.AllowUpgrades = opts.AllowUpgrades || containsString(opts.Transports, "websocket")
	return opts
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (o Options) transportsEnabled() map[string]bool {
	m := make(map[string]bool, len(o.Transports))
	for _, t := range o.Transports {
		m[t] = true
	}
	return m
}

// Context is a shorthand for context.Context to keep the public API terse.
type Context = context.Context
