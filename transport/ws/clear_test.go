package ws

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestClearThenNoBinary(t *testing.T) {
	tr := NewTransport(16000, nil)
	ready := make(chan struct{})
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		tr.setConn(c)
		close(ready)
		<-hold
		_ = c.Close()
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept")
	}

	out := tr.Output().(*wsOutput)
	if err := out.Setup(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Cleanup(context.Background()) }()

	type msg struct {
		mt   int
		data []byte
	}
	msgs := make(chan msg, 32)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
			mt, data, err := client.ReadMessage()
			if err != nil {
				return
			}
			msgs <- msg{mt: mt, data: append([]byte(nil), data...)}
		}
	}()

	pcm := bytes.Repeat([]byte{1, 0}, 16000) // 1s
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) {}}
	if err := out.Process(context.Background(), &frames.TTSAudioRawFrame{
		Audio: pcm, SampleRate: 16000, NumChannels: 1,
	}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}

	gotBinary := false
	deadline := time.After(2 * time.Second)
	for !gotBinary {
		select {
		case m := <-msgs:
			if m.mt == websocket.BinaryMessage && len(m.data) > 0 {
				gotBinary = true
			}
		case <-deadline:
			t.Fatal("no binary audio before interrupt")
		}
	}

	if err := out.Process(context.Background(), &frames.InterruptionFrame{}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}

	deadline = time.After(2 * time.Second)
	for {
		select {
		case m := <-msgs:
			if m.mt == websocket.TextMessage && string(m.data) == frames.TransportClearPayload {
				goto cleared
			}
		case <-deadline:
			t.Fatal("missing clear message")
		}
	}
cleared:
	select {
	case m := <-msgs:
		if m.mt == websocket.BinaryMessage {
			t.Fatalf("binary %d bytes after clear", len(m.data))
		}
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-hold:
	default:
		close(hold)
	}
	_ = client.Close()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
	}
}
