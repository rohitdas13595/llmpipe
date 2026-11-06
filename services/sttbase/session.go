// Package sttbase is a reconnecting websocket session for streaming STT.
// Audio is kept in a short ring and replayed after a drop. A keepalive is
// written when the socket has been idle. After MaxFailures consecutive
// failed dials or immediate disconnects, OnPermanentError is called once.
package sttbase

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Conn is the subset of a websocket connection the session uses.
type Conn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
}

// Config builds a Session.
type Config struct {
	Dial             func(ctx context.Context) (Conn, error)
	KeepAliveEvery   time.Duration
	KeepAlivePayload []byte
	RingBytes        int
	MaxFailures      int
	Backoff          func(attempt int) time.Duration
	OnMessage        func(messageType int, data []byte)
	OnPermanentError func(error)
}

// Session owns one streaming recognition socket.
type Session struct {
	cfg Config

	mu        sync.Mutex
	wmu       sync.Mutex
	conn      Conn
	ring      []byte
	pending   [][]byte
	lastWrite time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	started   bool
	closed    bool
	permOnce  sync.Once
}

// New returns a session that is idle until Start.
func New(cfg Config) *Session {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 5
	}
	if cfg.RingBytes <= 0 {
		cfg.RingBytes = 16000 * 2 * 2
	}
	if cfg.Backoff == nil {
		cfg.Backoff = defaultBackoff
	}
	return &Session{cfg: cfg}
}

// Start dials and keeps the socket up until Close or ctx is cancelled.
func (s *Session) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.run()
	if s.cfg.KeepAliveEvery > 0 && len(s.cfg.KeepAlivePayload) > 0 {
		s.wg.Add(1)
		go s.keepalive()
	}
	s.mu.Unlock()
}

// WriteAudio appends PCM to the replay ring and sends it when connected.
func (s *Session) WriteAudio(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	s.mu.Lock()
	s.ring = append(s.ring, p...)
	if extra := len(s.ring) - s.cfg.RingBytes; extra > 0 {
		if extra%2 == 1 {
			extra++
		}
		if extra > len(s.ring) {
			extra = len(s.ring)
		}
		s.ring = append([]byte(nil), s.ring[extra:]...)
	}
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return nil
	}
	return s.writeRaw(conn, websocket.BinaryMessage, p)
}

// WriteText sends a JSON control message, or queues it until the next connect.
func (s *Session) WriteText(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	s.mu.Lock()
	conn := s.conn
	if conn == nil {
		if len(s.pending) < 16 {
			s.pending = append(s.pending, append([]byte(nil), p...))
		}
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.writeRaw(conn, websocket.TextMessage, p)
}

// Close stops reconnects and waits for the read loop.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Session) run() {
	defer s.wg.Done()
	failures := 0
	for {
		if s.ctx.Err() != nil {
			return
		}
		conn, err := s.cfg.Dial(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			failures++
			if failures >= s.cfg.MaxFailures {
				s.permanent(err)
				return
			}
			if !sleep(s.ctx, s.cfg.Backoff(failures)) {
				return
			}
			continue
		}
		s.mu.Lock()
		s.conn = conn
		replay := append([]byte(nil), s.ring...)
		pending := s.pending
		s.pending = nil
		s.mu.Unlock()
		if len(replay) > 0 {
			_ = s.writeRaw(conn, websocket.BinaryMessage, replay)
		}
		for _, p := range pending {
			_ = s.writeRaw(conn, websocket.TextMessage, p)
		}
		got := s.read(conn)
		s.mu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.mu.Unlock()
		_ = conn.Close()
		if s.ctx.Err() != nil {
			return
		}
		if got {
			failures = 0
		} else {
			failures++
			if failures >= s.cfg.MaxFailures {
				s.permanent(errDialDropped)
				return
			}
		}
		if !sleep(s.ctx, s.cfg.Backoff(failures)) {
			return
		}
	}
}

func (s *Session) read(conn Conn) (got bool) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return got
		}
		got = true
		if s.cfg.OnMessage == nil {
			continue
		}
		s.cfg.OnMessage(mt, append([]byte(nil), data...))
	}
}

func (s *Session) keepalive() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.KeepAliveEvery)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
			s.mu.Lock()
			conn := s.conn
			idle := time.Since(s.lastWrite)
			payload := s.cfg.KeepAlivePayload
			s.mu.Unlock()
			if conn == nil || len(payload) == 0 {
				continue
			}
			if !s.lastWrite.IsZero() && idle < s.cfg.KeepAliveEvery {
				continue
			}
			_ = s.writeRaw(conn, websocket.TextMessage, payload)
		}
	}
}

func (s *Session) writeRaw(conn Conn, mt int, p []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	current := s.conn
	s.mu.Unlock()
	if current != nil && current != conn {
		return nil
	}
	err := conn.WriteMessage(mt, p)
	if err == nil {
		s.mu.Lock()
		s.lastWrite = time.Now()
		s.mu.Unlock()
	}
	return err
}

func (s *Session) permanent(err error) {
	s.permOnce.Do(func() {
		if s.cfg.OnPermanentError != nil && err != nil {
			s.cfg.OnPermanentError(err)
		}
	})
}

type dialDropped string

func (e dialDropped) Error() string { return string(e) }

const errDialDropped dialDropped = "stt: socket closed before any message"

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt
	if shift > 5 {
		shift = 5
	}
	d := time.Duration(100*(1<<shift)) * time.Millisecond
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = time.Millisecond
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
