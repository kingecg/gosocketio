package engineio

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"

	"github.com/coder/websocket"
	"github.com/kingecg/gosocketio/engineio/transport"
)

// errorMessages maps protocol error codes to their descriptions, mirroring
// the reference implementation.
var errorMessages = map[int]string{
	ErrCodeUnknownTransport:           "Transport unknown",
	ErrCodeUnknownSID:                 "Session ID unknown",
	ErrCodeBadHandshakeMethod:         "Bad handshake method",
	ErrCodeBadRequest:                 "Bad request",
	ErrCodeForbidden:                  "Forbidden",
	ErrCodeUnsupportedProtocolVersion: "Unsupported protocol version",
}

// Server is an Engine.IO server. It implements http.Handler and can be
// mounted on any URL path.
type Server struct {
	Options Options
	logger  Logger

	mu      sync.RWMutex
	clients map[string]*Socket

	onConnect func(s *Socket)
	onData    func(s *Socket, data []byte, binary bool)
	onClose   func(s *Socket, reason string, err error)

	// AcceptOptions customizes the WebSocket upgrade. Defaults to allowing
	// all origins (matching the reference implementation).
	AcceptOptions *websocket.AcceptOptions
}

// NewServer creates an Engine.IO server.
func NewServer(opts *Options) *Server {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o = o.withDefaults()
	return &Server{
		Options:       o,
		logger:        defaultLogger,
		clients:       make(map[string]*Socket),
		AcceptOptions: &websocket.AcceptOptions{InsecureSkipVerify: true},
	}
}

// SetLogger sets the server logger.
func (s *Server) SetLogger(l Logger) {
	if l != nil {
		s.logger = l
	}
}

// OnConnect registers a handler invoked once a new session is established.
func (s *Server) OnConnect(f func(s *Socket)) {
	s.onConnect = f
}

// OnData registers a handler invoked for every received `message` packet.
func (s *Server) OnData(f func(s *Socket, data []byte, binary bool)) {
	s.onData = f
}

// OnClose registers a handler invoked when a session is closed.
func (s *Server) OnClose(f func(s *Socket, reason string, err error)) {
	s.onClose = f
}

// Close closes all active sessions.
func (s *Server) Close() {
	s.mu.RLock()
	sockets := make([]*Socket, 0, len(s.clients))
	for _, c := range s.clients {
		sockets = append(sockets, c)
	}
	s.mu.RUnlock()
	for _, c := range sockets {
		c.Close()
	}
}

// ClientsCount returns the number of active sessions.
func (s *Server) ClientsCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	transportName := q.Get("transport")

	switch transportName {
	case "polling":
		s.handlePolling(w, r, q)
	case "websocket":
		s.handleWebsocket(w, r, q)
	default:
		s.abortRequest(w, ErrCodeUnknownTransport, nil)
	}
}

func (s *Server) handlePolling(w http.ResponseWriter, r *http.Request, q url.Values) {
	if err := s.verify(r, q, false); err != nil {
		s.abortRequest(w, errorCode(err), nil)
		return
	}

	sid := q.Get("sid")
	if sid == "" {
		s.handshake("polling", w, r)
		return
	}

	s.mu.RLock()
	sock := s.clients[sid]
	s.mu.RUnlock()
	if sock == nil {
		s.abortRequest(w, ErrCodeUnknownSID, nil)
		return
	}
	if p, ok := sock.Transport().(transport.HTTPServing); ok {
		p.ServeHTTP(w, r)
		return
	}
	// A message POSTed over polling just before a websocket upgrade may reach
	// the server after the active transport has switched. Route it to the
	// retained polling transport so the payload still reaches the socket
	// (which forwards replies over the now-active transport).
	if pt := sock.pollingTransport(); pt != nil {
		if hp, ok := pt.(transport.HTTPServing); ok {
			hp.ServeHTTP(w, r)
			return
		}
	}
	s.abortRequest(w, ErrCodeBadRequest, nil)
}

func (s *Server) handleWebsocket(w http.ResponseWriter, r *http.Request, q url.Values) {
	if err := s.verify(r, q, true); err != nil {
		s.abortUpgrade(w, errorCode(err))
		return
	}

	sid := q.Get("sid")
	if sid != "" {
		s.mu.RLock()
		sock := s.clients[sid]
		s.mu.RUnlock()
		if sock == nil {
			s.abortUpgrade(w, ErrCodeUnknownSID)
			return
		}
		ws, err := websocket.Accept(w, r, s.AcceptOptions)
		if err != nil {
			return
		}
		wt := transport.NewWebsocket(context.Background(), ws)
		wt.SetLogger(s.logger)
		sock.MaybeUpgrade(wt)
		return
	}

	// Direct websocket handshake (no prior polling session).
	ws, err := websocket.Accept(w, r, s.AcceptOptions)
	if err != nil {
		return
	}
	wt := transport.NewWebsocket(context.Background(), ws)
	wt.SetLogger(s.logger)
	s.handshakeWebsocket(wt, r)
}

func (s *Server) handshake(transportName string, w http.ResponseWriter, r *http.Request) {
	pt := transport.NewPolling()
	pt.SetMaxBufferSize(s.Options.MaxHTTPBufferSize)
	pt.SetLogger(s.logger)
	var tport transport.Transport = pt
	s.createSocket(tport, r, w)
	if p, ok := tport.(transport.HTTPServing); ok {
		p.ServeHTTP(w, r)
	}
}

func (s *Server) handshakeWebsocket(wt *transport.Websocket, r *http.Request) {
	s.createSocket(wt, r, nil)
}

func (s *Server) createSocket(tport transport.Transport, r *http.Request, w http.ResponseWriter) *Socket {
	id := s.generateID(r)
	sock := &Socket{
		id:         id,
		server:     s,
		opts:       s.Options,
		protocol:   Protocol,
		logger:     s.logger,
		remoteAddr: remoteAddr(r),
	}
	sock.onConnect = s.onConnect
	sock.onData = s.onData
	sock.onClose = func(sock *Socket, reason string, err error) {
		s.mu.Lock()
		delete(s.clients, sock.id)
		s.mu.Unlock()
		if s.onClose != nil {
			s.onClose(sock, reason, err)
		}
	}

	s.mu.Lock()
	s.clients[id] = sock
	s.mu.Unlock()

	sock.attach(tport)
	sock.open()

	if s.onConnect != nil {
		s.onConnect(sock)
	}
	return sock
}

// verify validates an HTTP or upgrade request. It mirrors the reference
// implementation's checks.
func (s *Server) verify(r *http.Request, q url.Values, upgrade bool) error {
	transportName := q.Get("transport")

	if q.Get("EIO") != "4" {
		return ErrUnsupportedProtocol
	}
	if !s.Options.transportsEnabled()[transportName] {
		return ErrUnknownTransport
	}

	sid := q.Get("sid")
	if sid != "" {
		s.mu.RLock()
		sock := s.clients[sid]
		s.mu.RUnlock()
		if sock == nil {
			return ErrUnknownSID
		}
		prev := sock.Transport().Name()
		if !upgrade && prev != transportName {
			return ErrBadRequest
		}
		return nil
	}

	if r.Method != http.MethodGet {
		return ErrBadHandshakeMethod
	}
	if transportName == "websocket" && !upgrade {
		return ErrBadRequest
	}
	if s.Options.AllowRequest != nil {
		if err := s.Options.AllowRequest(r); err != nil {
			return ErrForbidden
		}
	}
	return nil
}

func (s *Server) upgradesFor(transportName string) []string {
	if !s.Options.AllowUpgrades {
		return []string{}
	}
	if transportName == "polling" && s.Options.transportsEnabled()["websocket"] {
		return []string{"websocket"}
	}
	return []string{}
}

func (s *Server) generateID(r *http.Request) string {
	if s.Options.GenerateID != nil {
		return s.Options.GenerateID(r)
	}
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawStdEncoding.EncodeToString(b)
}

func (s *Server) abortRequest(w http.ResponseWriter, code int, ctx map[string]any) {
	status := http.StatusBadRequest
	if code == ErrCodeForbidden {
		status = http.StatusForbidden
	}
	msg := errorMessages[code]
	if ctx != nil {
		if m, ok := ctx["message"].(string); ok {
			msg = m
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "message": msg})
}

func (s *Server) abortUpgrade(w http.ResponseWriter, code int) {
	// Write a minimal plain HTTP error; the WebSocket client will see it as
	// a failed handshake.
	s.abortRequest(w, code, nil)
}

func errorCode(err error) int {
	switch err {
	case ErrUnsupportedProtocol:
		return ErrCodeUnsupportedProtocolVersion
	case ErrUnknownTransport:
		return ErrCodeUnknownTransport
	case ErrUnknownSID:
		return ErrCodeUnknownSID
	case ErrBadHandshakeMethod:
		return ErrCodeBadHandshakeMethod
	case ErrForbidden:
		return ErrCodeForbidden
	default:
		return ErrCodeBadRequest
	}
}

func remoteAddr(r *http.Request) string {
	if r == nil {
		return ""
	}
	return r.RemoteAddr
}
