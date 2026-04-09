// Package ws provides a WebSocket PCM transport with Input/Output processors.
package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// Transport serves browser/raw PCM clients (16-bit LE mono).
type Transport struct {
	SampleRate int
	mu         sync.RWMutex
	conn       *websocket.Conn
	queue      func(context.Context, []frames.Frame) error
}

// NewTransport creates a WS transport. queue injects frames at pipeline start (typically task.QueueFrames).
func NewTransport(sampleRate int, queue func(context.Context, []frames.Frame) error) *Transport {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &Transport{SampleRate: sampleRate, queue: queue}
}

func (t *Transport) setConn(c *websocket.Conn) {
	t.mu.Lock()
	t.conn = c
	t.mu.Unlock()
}

// Input is a passthrough processor (audio arrives via QueueFrames from HTTP handler).
func (t *Transport) Input() processor.Processor {
	return processor.Func{
		N: "ws.input",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			emit.Down(f)
			return nil
		},
	}
}

// Output writes TTSAudioRawFrame to the active WebSocket as binary messages.
func (t *Transport) Output() processor.Processor {
	return &wsOutput{t: t}
}

type wsOutput struct {
	t *Transport
}

func (o *wsOutput) Name() string { return "ws.output" }

func (o *wsOutput) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if a, ok := f.(*frames.TTSAudioRawFrame); ok {
		// Serialize writes; concurrent WriteMessage is unsafe on gorilla/websocket.
		o.t.mu.Lock()
		c := o.t.conn
		if c != nil {
			if err := c.WriteMessage(websocket.BinaryMessage, a.Audio); err != nil {
				o.t.mu.Unlock()
				emit.Down(&frames.ErrorFrame{Err: err})
				emit.Down(f)
				return nil
			}
		}
		o.t.mu.Unlock()
	}
	emit.Down(f)
	return nil
}

// Upgrader is used by HandleWebSocket.
var Upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// HandleWebSocket upgrades HTTP, stores conn, and reads binary PCM into the pipeline.
func (t *Transport) HandleWebSocket(w http.ResponseWriter, r *http.Request) error {
	c, err := Upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}
	t.setConn(c)
	defer func() {
		t.setConn(nil)
		_ = c.Close()
	}()

	if t.queue != nil {
		_ = t.queue(r.Context(), []frames.Frame{
			&frames.StartFrame{SampleRate: t.SampleRate, NumChannels: 1},
		})
	}

	for {
		mt, data, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if mt == websocket.TextMessage && t.queue != nil {
			if handleWSControl(data, t.queue) {
				continue
			}
		}
		if mt != websocket.BinaryMessage || len(data) == 0 {
			continue
		}
		if t.queue != nil {
			_ = t.queue(context.Background(), []frames.Frame{
				&frames.InputAudioRawFrame{
					Audio:       data,
					SampleRate:  t.SampleRate,
					NumChannels: 1,
				},
			})
		}
	}
}

// wsControlJSON is a small client → server control payload (demo UI "End utterance").
type wsControlJSON struct {
	EndUtterance bool `json:"endUtterance"`
}

// handleWSControl returns true if the message was consumed as a control (not PCM).
func handleWSControl(data []byte, queue func(context.Context, []frames.Frame) error) bool {
	s := strings.TrimSpace(string(data))
	if s == "" {
		return false
	}
	var c wsControlJSON
	if json.Unmarshal(data, &c) == nil && c.EndUtterance {
		_ = queue(context.Background(), []frames.Frame{&frames.UserStoppedSpeakingFrame{}})
		return true
	}
	// Plain-text fallback for quick tests
	if strings.EqualFold(s, "end") || strings.EqualFold(s, "end_utterance") {
		_ = queue(context.Background(), []frames.Frame{&frames.UserStoppedSpeakingFrame{}})
		return true
	}
	return false
}
