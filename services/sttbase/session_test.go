package sttbase

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestKeepAlive(t *testing.T) {
	got := make(chan []byte, 1)
	srv := serveWS(t, func(c *websocket.Conn) {
		for {
			mt, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.TextMessage {
				got <- append([]byte(nil), msg...)
				return
			}
		}
	})
	defer srv.Close()
	sess := New(Config{
		Dial:             dialer(srv),
		KeepAliveEvery:   20 * time.Millisecond,
		KeepAlivePayload: []byte(`{"type":"KeepAlive"}`),
		Backoff:          func(int) time.Duration { return time.Millisecond },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess.Start(ctx)
	defer sess.Close()
	select {
	case msg := <-got:
		if string(msg) != `{"type":"KeepAlive"}` {
			t.Fatalf("keepalive %s", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no keepalive")
	}
}

func TestReconnectReplaysRing(t *testing.T) {
	payload := []byte{9, 8, 7, 6}
	var mu sync.Mutex
	n := 0
	first := make(chan []byte, 1)
	second := make(chan []byte, 1)
	srv := serveWS(t, func(c *websocket.Conn) {
		mu.Lock()
		n++
		id := n
		mu.Unlock()
		defer c.Close()
		_, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		cp := append([]byte(nil), msg...)
		if id == 1 {
			first <- cp
			return
		}
		second <- cp
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()
	sess := New(Config{
		Dial:      dialer(srv),
		RingBytes: 8000,
		Backoff:   func(int) time.Duration { return 5 * time.Millisecond },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess.Start(ctx)
	defer sess.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		_ = sess.WriteAudio(payload)
		select {
		case got := <-first:
			if !bytesContains(got, payload) {
				t.Fatalf("first audio %v", got)
			}
			goto replayed
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("first connection did not see audio")
		}
		time.Sleep(5 * time.Millisecond)
	}
replayed:
	select {
	case got := <-second:
		if !bytesContains(got, payload) {
			t.Fatalf("replay %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not replay the ring")
	}
}

func TestPermanentError(t *testing.T) {
	done := make(chan error, 1)
	var n atomic.Int32
	sess := New(Config{
		Dial: func(context.Context) (Conn, error) {
			return nil, errors.New("dial failed")
		},
		MaxFailures: 3,
		Backoff:     func(int) time.Duration { return time.Millisecond },
		OnPermanentError: func(err error) {
			n.Add(1)
			done <- err
		},
	})
	sess.Start(context.Background())
	defer sess.Close()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "dial failed") {
			t.Fatalf("err %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no permanent error")
	}
	time.Sleep(20 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("callbacks %d", n.Load())
	}
}

func bytesContains(hay, needle []byte) bool {
	if len(needle) == 0 || len(hay) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(hay); i++ {
		match := true
		for j := range needle {
			if hay[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func serveWS(t *testing.T, h func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		h(c)
	}))
}

func dialer(srv *httptest.Server) func(context.Context) (Conn, error) {
	u := "ws" + strings.TrimPrefix(srv.URL, "http")
	return func(ctx context.Context) (Conn, error) {
		c, _, err := websocket.DefaultDialer.DialContext(ctx, u, nil)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
}
