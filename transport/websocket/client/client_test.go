package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rohitdas13595/llmpipe/frames"
)

func TestTransportConnectQueuesStartAndAudio(t *testing.T) {
	var up = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		_ = c.WriteMessage(websocket.BinaryMessage, []byte{0x00, 0x01})
	}))
	defer srv.Close()

	wsURL := strings.Replace(srv.URL, "http", "ws", 1)
	var n atomic.Int32
	queue := func(_ context.Context, fs []frames.Frame) error {
		n.Add(int32(len(fs)))
		return nil
	}
	tr := NewTransport(16000, wsURL, queue)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	_ = tr.Close()
	if got := n.Load(); got < 2 {
		t.Fatalf("expected StartFrame + at least one audio chunk, got %d", got)
	}
}
