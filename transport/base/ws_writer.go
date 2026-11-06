package base

import (
	"net"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/rohitdas13595/llmpipe/frames"
)

type audioMsg struct {
	data  []byte
	epoch uint64
}

// WSWriter serializes websocket writes onto one goroutine. Clear drops any
// audio already queued and sends {"type":"clear"}.
type WSWriter struct {
	get    func() *websocket.Conn
	audio  chan audioMsg
	clear  chan chan error
	stop   chan struct{}
	exited chan struct{}

	mu     sync.Mutex
	epoch  uint64
	closed bool
}

// NewWSWriter starts the writer. get is called for each write so a reconnect
// is visible. Stop must be called to release the goroutine.
func NewWSWriter(get func() *websocket.Conn) *WSWriter {
	w := &WSWriter{
		get:    get,
		audio:  make(chan audioMsg, 8),
		clear:  make(chan chan error),
		stop:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	go w.loop()
	return w
}

// Write queues a binary PCM chunk. Audio queued before the latest Clear is dropped.
func (w *WSWriter) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return net.ErrClosed
	}
	ep := w.epoch
	w.mu.Unlock()
	cp := make([]byte, len(b))
	copy(cp, b)
	select {
	case w.audio <- audioMsg{data: cp, epoch: ep}:
		return nil
	case <-w.stop:
		return net.ErrClosed
	}
}

// Clear drops queued audio and writes the clear text message.
func (w *WSWriter) Clear() error {
	done := make(chan error, 1)
	select {
	case w.clear <- done:
	case <-w.stop:
		return net.ErrClosed
	}
	return <-done
}

// Stop ends the writer goroutine.
func (w *WSWriter) Stop() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	close(w.stop)
	w.mu.Unlock()
	<-w.exited
}

func (w *WSWriter) loop() {
	defer close(w.exited)
	for {
		select {
		case <-w.stop:
			return
		case done := <-w.clear:
			w.finishClear(done)
			continue
		default:
		}
		select {
		case <-w.stop:
			return
		case done := <-w.clear:
			w.finishClear(done)
		case m := <-w.audio:
			w.mu.Lock()
			cur := w.epoch
			w.mu.Unlock()
			if m.epoch != cur {
				continue
			}
			_ = w.writeMsg(websocket.BinaryMessage, m.data)
		}
	}
}

func (w *WSWriter) finishClear(done chan error) {
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	w.drain()
	err := w.writeMsg(websocket.TextMessage, []byte(frames.TransportClearPayload))
	done <- err
}

func (w *WSWriter) drain() {
	for {
		select {
		case <-w.audio:
		default:
			return
		}
	}
}

func (w *WSWriter) writeMsg(mt int, b []byte) error {
	if w.get == nil {
		return nil
	}
	c := w.get()
	if c == nil {
		return nil
	}
	return c.WriteMessage(mt, b)
}
