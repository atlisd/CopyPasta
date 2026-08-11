// Package api exposes the HTTP surface: the embedded UI, the JSON endpoints the
// page talks to, and the WebSocket the receiving machine listens on.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"copypasta/internal/store"
)

// AllowedTTLs are the lifetimes the UI offers; anything else is rejected.
var AllowedTTLs = []time.Duration{time.Minute, 10 * time.Minute, time.Hour}

// Config carries the server knobs read from the environment.
type Config struct {
	MaxPasteBytes int64
	DefaultTTL    time.Duration
}

type Server struct {
	store  *store.Store
	cfg    Config
	web    fs.FS
	limits *rateLimiter
}

func NewServer(st *store.Store, cfg Config, web fs.FS) *Server {
	return &Server{store: st, cfg: cfg, web: web, limits: newRateLimiter(20, time.Minute)}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/channel", s.handleCreateChannel)
	mux.HandleFunc("POST /api/publish", s.handlePublish)
	mux.HandleFunc("POST /api/fetch", s.handleFetch)
	mux.HandleFunc("GET /api/subscribe", s.handleSubscribe)
	mux.Handle("GET /", http.FileServer(http.FS(s.web)))
	return noStore(mux)
}

// noStore keeps pastes out of browser caches and search indexes.
func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Header().Set("X-Robots-Tag", "noindex, nofollow")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- Sending side -------------------------------------------------------

type createRequest struct {
	TTL int `json:"ttl"` // seconds
}

type createResponse struct {
	PIN       string `json:"pin"`
	Token     string `json:"token"`
	ExpiresIn int    `json:"expires_in"`
}

// handleCreateChannel hands the sender a PIN it can read out loud and a write
// token only it holds.
func (s *Server) handleCreateChannel(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<12)

	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "malformed request")
		return
	}

	ttl, err := s.resolveTTL(req.TTL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "unsupported expiry")
		return
	}

	pin, token, err := s.store.Create(r.Context(), ttl)
	if err != nil {
		log.Printf("create channel: %v", err)
		writeError(w, http.StatusInternalServerError, "could not open a channel")
		return
	}

	writeJSON(w, http.StatusOK, createResponse{PIN: pin, Token: token, ExpiresIn: int(ttl.Seconds())})
}

type publishRequest struct {
	PIN   string `json:"pin"`
	Token string `json:"token"`
	Kind  string `json:"kind"`
	MIME  string `json:"mime"`
	Data  string `json:"data"`
	TTL   int    `json:"ttl"`
}

type publishResponse struct {
	ExpiresIn int `json:"expires_in"`
}

// handlePublish pushes a new value onto an open channel. Every listener sees it
// immediately; the channel's expiry slides out to a full TTL again.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxPasteBytes)

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) || errors.Is(err, io.ErrUnexpectedEOF) {
			writeError(w, http.StatusRequestEntityTooLarge, "paste is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed request")
		return
	}

	if !validPIN(req.PIN) {
		writeError(w, http.StatusBadRequest, "pin must be 6 digits")
		return
	}
	if req.Data == "" {
		writeError(w, http.StatusBadRequest, "nothing to send")
		return
	}
	if req.Kind != "text" && req.Kind != "image" {
		writeError(w, http.StatusBadRequest, "unsupported kind")
		return
	}
	if req.Kind == "image" {
		if !strings.HasPrefix(req.MIME, "image/") {
			writeError(w, http.StatusBadRequest, "unsupported image type")
			return
		}
		if _, err := base64.StdEncoding.DecodeString(req.Data); err != nil {
			writeError(w, http.StatusBadRequest, "image data is not valid base64")
			return
		}
	} else {
		req.MIME = "text/plain"
	}

	ttl, err := s.resolveTTL(req.TTL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "unsupported expiry")
		return
	}

	err = s.store.Publish(r.Context(), req.PIN, req.Token, store.Paste{
		Kind: req.Kind,
		MIME: req.MIME,
		Data: req.Data,
	}, ttl)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "that channel has expired — create a new PIN")
		return
	case errors.Is(err, store.ErrBadToken):
		writeError(w, http.StatusForbidden, "not your channel")
		return
	case err != nil:
		log.Printf("publish: %v", err)
		writeError(w, http.StatusInternalServerError, "could not send")
		return
	}

	writeJSON(w, http.StatusOK, publishResponse{ExpiresIn: int(ttl.Seconds())})
}

// resolveTTL maps a requested lifetime in seconds onto the allowed set.
func (s *Server) resolveTTL(seconds int) (time.Duration, error) {
	if seconds == 0 {
		return s.cfg.DefaultTTL, nil
	}
	want := time.Duration(seconds) * time.Second
	for _, ttl := range AllowedTTLs {
		if ttl == want {
			return ttl, nil
		}
	}
	return 0, errors.New("ttl not allowed")
}

// ---- Receiving side -----------------------------------------------------

type fetchRequest struct {
	PIN string `json:"pin"`
}

// update is what the receiver gets, over both /api/fetch and the WebSocket.
type update struct {
	Kind      string `json:"kind"`
	MIME      string `json:"mime"`
	Data      string `json:"data"`
	ExpiresIn int    `json:"expires_in"`
	Empty     bool   `json:"empty,omitempty"` // channel is open but nothing sent yet
}

// handleFetch validates a PIN and returns whatever is on the channel right now.
// The UI calls it once when joining, then keeps up over the WebSocket.
func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<12)

	if !s.limits.allow(clientIP(r)) {
		writeError(w, http.StatusTooManyRequests, "too many attempts, slow down")
		return
	}

	var req fetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request")
		return
	}
	if !validPIN(req.PIN) {
		writeError(w, http.StatusBadRequest, "pin must be 6 digits")
		return
	}

	p, ttl, err := s.store.Get(r.Context(), req.PIN)
	switch {
	case errors.Is(err, store.ErrEmpty):
		writeJSON(w, http.StatusOK, update{ExpiresIn: int(ttl.Seconds()), Empty: true})
		return
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "no channel for that pin (wrong, or it expired)")
		return
	case errors.Is(err, store.ErrLockedOut):
		writeError(w, http.StatusTooManyRequests, "that pin is locked after too many wrong attempts")
		return
	case err != nil:
		log.Printf("get: %v", err)
		writeError(w, http.StatusInternalServerError, "could not read channel")
		return
	}

	writeJSON(w, http.StatusOK, update{
		Kind:      p.Kind,
		MIME:      p.MIME,
		Data:      p.Data,
		ExpiresIn: int(ttl.Seconds()),
	})
}

// handleSubscribe upgrades to a WebSocket and streams every value pushed to the
// PIN, so the receiving machine updates the instant the sender hits Send.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	pin := r.URL.Query().Get("pin")
	if !validPIN(pin) {
		writeError(w, http.StatusBadRequest, "pin must be 6 digits")
		return
	}
	if !s.limits.allow(clientIP(r)) {
		writeError(w, http.StatusTooManyRequests, "too many attempts, slow down")
		return
	}

	// Prove the PIN is real before spending a connection on it. This also runs
	// the wrong-guess counter, so the socket is no easier to brute-force.
	snapshot, ttl, err := s.store.Get(r.Context(), pin)
	empty := errors.Is(err, store.ErrEmpty)
	if err != nil && !empty {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "no channel for that pin")
		case errors.Is(err, store.ErrLockedOut):
			writeError(w, http.StatusTooManyRequests, "that pin is locked")
		default:
			log.Printf("subscribe precheck: %v", err)
			writeError(w, http.StatusInternalServerError, "could not join channel")
		}
		return
	}

	// A subscriber must never outlive the channel it is watching.
	ctx, cancel := context.WithTimeout(r.Context(), ttl+time.Minute)
	defer cancel()

	// Subscribe before the handshake completes. Redis pub/sub has no replay, so
	// a value published while we were still setting up would be lost forever.
	updates, unsubscribe, err := s.store.Subscribe(ctx, pin)
	if err != nil {
		log.Printf("subscribe: %v", err)
		writeError(w, http.StatusInternalServerError, "could not join channel")
		return
	}
	defer unsubscribe()

	// Default options enforce same-origin: the browser's Origin must match the
	// Host it connected to. That holds on localhost and behind the tunnel alike.
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("ws accept: %v", err)
		return
	}
	defer conn.CloseNow()

	// Hand over whatever is already on the channel, so a receiver joining late
	// sees the current value without waiting for the sender to press Send again.
	if !empty {
		writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
		err := wsjson.Write(writeCtx, conn, update{
			Kind: snapshot.Kind, MIME: snapshot.MIME, Data: snapshot.Data,
			ExpiresIn: int(ttl.Seconds()),
		})
		cancelWrite()
		if err != nil {
			return
		}
	}

	// Notice the browser walking away, so we do not hold a Redis subscription open.
	go func() {
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				cancel()
				return
			}
		}
	}()

	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "expired")
			return

		case p, ok := <-updates:
			if !ok {
				return
			}
			left, err := s.store.TTL(ctx, pin)
			if err != nil {
				return
			}
			msg := update{Kind: p.Kind, MIME: p.MIME, Data: p.Data, ExpiresIn: int(left.Seconds())}
			writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
			err = wsjson.Write(writeCtx, conn, msg)
			cancelWrite()
			if err != nil {
				return
			}

		case <-ping.C:
			pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Ping(pingCtx)
			cancelPing()
			if err != nil {
				return
			}
		}
	}
}

func validPIN(pin string) bool {
	if len(pin) != 6 {
		return false
	}
	for _, c := range pin {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func clientIP(r *http.Request) string {
	// Behind the Cloudflare tunnel the real client shows up in these headers.
	if v := r.Header.Get("CF-Connecting-IP"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		return strings.TrimSpace(v)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// rateLimiter caps join attempts per client over a rolling window. It is
// per-process and in-memory, which is all a single-container deployment needs.
type rateLimiter struct {
	mu     sync.Mutex
	seen   map[string]*bucket
	limit  int
	window time.Duration
}

type bucket struct {
	count int
	reset time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{seen: make(map[string]*bucket), limit: limit, window: window}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	// Drop stale entries so the map cannot grow without bound.
	for k, b := range rl.seen {
		if now.After(b.reset) {
			delete(rl.seen, k)
		}
	}

	b, ok := rl.seen[key]
	if !ok {
		rl.seen[key] = &bucket{count: 1, reset: now.Add(rl.window)}
		return true
	}
	if b.count >= rl.limit {
		return false
	}
	b.count++
	return true
}
