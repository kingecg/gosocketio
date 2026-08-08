package engineio

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/kingecg/gosocketio/engineio/transport"
)

type readyState int

const (
	stateOpening readyState = iota
	stateOpen
	stateClosing
	stateClosed
)

// Socket represents a single Engine.IO session on the server side.
type Socket struct {
	id         string
	server     *Server
	opts       Options
	protocol   int
	logger     Logger
	remoteAddr string

	mu          sync.Mutex
	readyState  readyState
	transport   transport.Transport
	binding     *transportBinding
	writeBuffer []*transport.Packet
	upgrading   bool
	upgraded    bool
	sending     bool

	pingIntervalTimer *time.Timer
	pingTimeoutTimer  *time.Timer

	onConnect func(s *Socket)
	onData    func(s *Socket, data []byte, binary bool)
	onClose   func(s *Socket, reason string, err error)
}

// transportBinding dispatches transport events to the socket while
// remembering which transport they came from, so that events from discarded
// transports (e.g. the polling transport during a websocket upgrade) do not
// kill the session.
type transportBinding struct {
	socket    *Socket
	transport transport.Transport
}

func (b *transportBinding) OnPacket(p *transport.Packet) { b.socket.onPacket(p) }
func (b *transportBinding) OnError(err error)            { b.socket.onErrorFrom(b.transport, err) }
func (b *transportBinding) OnClose()                     { b.socket.onCloseFrom(b.transport) }
func (b *transportBinding) OnDrain()                     {}
func (b *transportBinding) OnReady()                     { b.socket.flush() }

// ID returns the session identifier.
func (s *Socket) ID() string { return s.id }

// Transport returns the currently active transport.
func (s *Socket) Transport() transport.Transport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transport
}

// ReadyState returns a human readable session state.
func (s *Socket) ReadyState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.readyState {
	case stateOpening:
		return "opening"
	case stateOpen:
		return "open"
	case stateClosing:
		return "closing"
	default:
		return "closed"
	}
}

// RemoteAddr returns the remote address of the client.
func (s *Socket) RemoteAddr() string {
	// best effort: expose through the transport-independent stored value
	return s.remoteAddr
}

func (s *Socket) attach(t transport.Transport) {
	b := &transportBinding{socket: s, transport: t}
	s.mu.Lock()
	s.transport = t
	s.binding = b
	s.mu.Unlock()
	t.SetHandler(b)
}

// open transitions the socket to the open state, sends the handshake `open`
// packet and starts the heartbeat.
func (s *Socket) open() {
	s.mu.Lock()
	s.readyState = stateOpen
	s.mu.Unlock()
	s.sendPacket(transport.Open, s.openPacketData(), false)
	s.schedulePing()
}

func (s *Socket) openPacketData() []byte {
	payload := struct {
		SID          string   `json:"sid"`
		Upgrades     []string `json:"upgrades"`
		PingInterval int64    `json:"pingInterval"`
		PingTimeout  int64    `json:"pingTimeout"`
		MaxPayload   int64    `json:"maxPayload"`
	}{
		SID:          s.id,
		Upgrades:     s.server.upgradesFor(s.transportName()),
		PingInterval: s.opts.PingInterval.Milliseconds(),
		PingTimeout:  s.opts.PingTimeout.Milliseconds(),
		MaxPayload:   s.opts.MaxHTTPBufferSize,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func (s *Socket) transportName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport == nil {
		return ""
	}
	return s.transport.Name()
}

// SendMessage sends a `message` packet to the client.
func (s *Socket) SendMessage(data []byte, binary bool) {
	fmt.Fprintf(os.Stderr, "[s:%s] echo %q\n", s.id, data)
	s.sendPacket(transport.Message, data, binary)
}

func (s *Socket) sendPacket(t transport.Type, data []byte, binary bool) {
	s.mu.Lock()
	if s.readyState == stateClosing || s.readyState == stateClosed {
		s.mu.Unlock()
		return
	}
	s.writeBuffer = append(s.writeBuffer, &transport.Packet{Type: t, Data: data, IsBinary: binary})
	tport := s.transport
	s.mu.Unlock()
	if tport != nil && tport.Writable() {
		s.flush()
	}
}

// flush delivers all buffered packets to the transport.
func (s *Socket) flush() {
	var reflush bool
	s.mu.Lock()
	if s.readyState == stateClosed || s.sending {
		s.mu.Unlock()
		return
	}
	tport := s.transport
	if tport == nil || len(s.writeBuffer) == 0 || !tport.Writable() {
		s.mu.Unlock()
		return
	}
	buf := s.writeBuffer
	s.writeBuffer = nil
	s.sending = true
	s.mu.Unlock()

	tport.Send(buf)

	s.mu.Lock()
	s.sending = false
	reflush = s.readyState != stateClosed && len(s.writeBuffer) > 0 && s.transport.Writable()
	s.mu.Unlock()
	if reflush {
		s.flush()
	}
}

func (s *Socket) onPacket(p *transport.Packet) {
	s.mu.Lock()
	if s.readyState != stateOpen {
		s.mu.Unlock()
		return
	}
	switch p.Type {
	case transport.Pong:
		if s.pingTimeoutTimer != nil {
			s.pingTimeoutTimer.Stop()
		}
		if s.pingIntervalTimer != nil {
			s.pingIntervalTimer.Stop()
		}
		s.pingIntervalTimer = time.AfterFunc(s.opts.PingInterval, s.pingHandler)
		s.mu.Unlock()
	case transport.Ping:
		// clients of protocol v4 must not send ping packets
		s.mu.Unlock()
		s.closeWithReason("invalid heartbeat direction")
	case transport.Message:
		onData := s.onData
		data := p.Data
		binary := p.IsBinary
		tn := "?"
		if s.transport != nil {
			tn = s.transport.Name()
		}
		fmt.Fprintf(os.Stderr, "[s:%s] recv msg %q on %s\n", s.id, data, tn)
		s.mu.Unlock()
		if onData != nil {
			onData(s, data, binary)
		}
	case transport.Close:
		s.mu.Unlock()
		s.closeWithReason("client close")
	default:
		s.mu.Unlock()
	}
}

func (s *Socket) onErrorFrom(t transport.Transport, err error) {
	s.mu.Lock()
	active := s.transport == t
	upgrading := s.upgrading
	s.mu.Unlock()
	if !active || upgrading {
		// Errors from a transport being torn down by an in-flight upgrade
		// are expected (the client aborts the old poll once the switch
		// happens) and must not kill the session.
		return
	}
	s.logger.Debugf("engineio[%s]: transport error: %v", s.id, err)
	s.closeWithReason("transport error")
}

func (s *Socket) onCloseFrom(t transport.Transport) {
	s.mu.Lock()
	active := s.transport == t
	upgrading := s.upgrading
	s.mu.Unlock()
	if !active || upgrading {
		return
	}
	s.closeWithReason("transport close")
}

func (s *Socket) schedulePing() {
	s.mu.Lock()
	s.pingIntervalTimer = time.AfterFunc(s.opts.PingInterval, s.pingHandler)
	s.mu.Unlock()
}

func (s *Socket) pingHandler() {
	s.mu.Lock()
	if s.readyState == stateClosed {
		s.mu.Unlock()
		return
	}
	if s.upgrading {
		// Heartbeat is paused during an upgrade; keep the interval alive
		// so it resumes once the upgrade settles.
		s.pingIntervalTimer = time.AfterFunc(s.opts.PingInterval, s.pingHandler)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.sendPacket(transport.Ping, nil, false)
	s.mu.Lock()
	s.pingTimeoutTimer = time.AfterFunc(s.opts.PingTimeout, s.pingTimeoutHandler)
	s.mu.Unlock()
}

func (s *Socket) pingTimeoutHandler() {
	s.mu.Lock()
	if s.readyState == stateClosed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.closeWithReason("ping timeout")
}

// Close closes the socket gracefully: pending packets are flushed and a
// `close` packet (or WebSocket close frame) is delivered to the client.
func (s *Socket) Close() {
	s.mu.Lock()
	if s.readyState != stateOpen {
		s.mu.Unlock()
		return
	}
	s.readyState = stateClosing
	s.mu.Unlock()
	s.flush()
	s.closeTransport()
}

func (s *Socket) closeTransport() {
	s.mu.Lock()
	tport := s.transport
	s.mu.Unlock()
	if tport != nil {
		tport.Close()
	}
}

// closeWithReason tears the session down and notifies the server once.
// It is reentrancy-safe: transport close events triggered from within the
// cleanup see the session already closed and return immediately.
func (s *Socket) closeWithReason(reason string) {
	s.mu.Lock()
	if s.readyState == stateClosed {
		s.mu.Unlock()
		return
	}
	s.readyState = stateClosed
	if s.pingIntervalTimer != nil {
		s.pingIntervalTimer.Stop()
	}
	if s.pingTimeoutTimer != nil {
		s.pingTimeoutTimer.Stop()
	}
	tport := s.transport
	onClose := s.onClose
	s.mu.Unlock()

	if tport != nil {
		tport.Discard()
		tport.Close()
	}
	if onClose != nil {
		onClose(s, reason, nil)
	}
}

// MaybeUpgrade is called when the client attempts to upgrade the current
// transport to the given one (websocket).
func (s *Socket) MaybeUpgrade(newTransport transport.Transport) {
	s.mu.Lock()
	if s.upgrading || s.upgraded || s.readyState != stateOpen {
		s.mu.Unlock()
		newTransport.Close()
		return
	}
	s.upgrading = true
	// Pause the heartbeat while the probe handshake is in flight: the
	// client stops polling during the upgrade so it cannot answer pings.
	if s.pingIntervalTimer != nil {
		s.pingIntervalTimer.Stop()
		s.pingIntervalTimer = nil
	}
	if s.pingTimeoutTimer != nil {
		s.pingTimeoutTimer.Stop()
		s.pingTimeoutTimer = nil
	}
	s.mu.Unlock()

	upgradeTimeout := time.AfterFunc(s.opts.UpgradeTimeout, func() {
		newTransport.Close()
	})

	stopCheck := make(chan struct{})
	var checkOnce sync.Once
	stopCheckFn := func() {
		checkOnce.Do(func() { close(stopCheck) })
	}

	up := &upgradeHandler{
		onProbe: func() {
			newTransport.Send([]*transport.Packet{{Type: transport.Pong, Data: []byte("probe")}})
			// Keep the polling transport cycling while the upgrade is in
			// flight, so no packet is lost if the upgrade fails.
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopCheck:
						return
					case <-ticker.C:
						if s.pollingWritable() {
							s.pollingNoop()
						}
					}
				}
			}()
		},
		onUpgrade: func() {
			upgradeTimeout.Stop()
			stopCheckFn()
			s.mu.Lock()
			if s.readyState == stateClosed {
				s.mu.Unlock()
				newTransport.Close()
				return
			}
			old := s.transport
			s.upgrading = false
			s.upgraded = true
			s.transport = newTransport
			s.mu.Unlock()

			if old != nil {
				old.Discard()
				old.Close()
			}
			// Re-bind the socket as the handler of the new transport.
			b := &transportBinding{socket: s, transport: newTransport}
			s.mu.Lock()
			s.binding = b
			s.mu.Unlock()
			newTransport.SetHandler(b)
			s.flush()
			s.mu.Lock()
			if s.readyState == stateOpen {
				s.pingIntervalTimer = time.AfterFunc(s.opts.PingInterval, s.pingHandler)
			}
			s.mu.Unlock()
		},
		onAbort: func() {
			upgradeTimeout.Stop()
			stopCheckFn()
			newTransport.Close()
			s.mu.Lock()
			s.upgrading = false
			if s.readyState == stateOpen {
				s.pingIntervalTimer = time.AfterFunc(s.opts.PingInterval, s.pingHandler)
			}
			s.mu.Unlock()
		},
	}
	newTransport.SetHandler(up)
}

func (s *Socket) pollingWritable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upgraded {
		return false
	}
	tport := s.transport
	return tport != nil && tport.Name() == "polling" && tport.Writable()
}

func (s *Socket) pollingNoop() {
	s.mu.Lock()
	tport := s.transport
	s.mu.Unlock()
	if tport != nil {
		tport.Send([]*transport.Packet{{Type: transport.Noop}})
	}
}

// upgradeHandler receives packets on a transport candidate while the
// upgrade probe handshake is in progress.
type upgradeHandler struct {
	onProbe   func()
	onUpgrade func()
	onAbort   func()
}

func (u *upgradeHandler) OnPacket(p *transport.Packet) {
	switch {
	case p.Type == transport.Ping && string(p.Data) == "probe":
		u.onProbe()
	case p.Type == transport.Upgrade:
		u.onUpgrade()
	default:
		u.onAbort()
	}
}

func (u *upgradeHandler) OnError(err error) { u.onAbort() }
func (u *upgradeHandler) OnClose()          { u.onAbort() }
func (u *upgradeHandler) OnDrain()          {}
func (u *upgradeHandler) OnReady()          {}
