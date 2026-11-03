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
	"github.com/rohitdas13595/llmpipe/transport/base"
)

// Transport serves browser/raw PCM clients (16-bit LE mono).
type Transport struct {
	SampleRate int
	mu         sync.RWMutex
	conn       *websocket.Conn
	queue      func(context.Context, []frames.Frame) error
	// OnDisconnect is called when the WebSocket closes (client gone or read error). Use it to stop the pipeline.
	OnDisconnect func()
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

// Output paces TTS audio onto the WebSocket and sends {"type":"clear"} when
// the pipeline is interrupted.
func (t *Transport) Output() processor.Processor {
	get := func() *websocket.Conn {
		t.mu.RLock()
		defer t.mu.RUnlock()
		return t.conn
	}
	w := base.NewWSWriter(get)
	ao := &base.AudioOutput{
		SampleRate: t.SampleRate,
		Write:      w.Write,
		Clear:      w.Clear,
	}
	return &wsOutput{ao: ao, w: w}
}

type wsOutput struct {
	ao *base.AudioOutput
	w  *base.WSWriter
}

func (o *wsOutput) Name() string { return "ws.output" }

func (o *wsOutput) Setup(ctx context.Context, h processor.Handle) error {
	o.ao.Start(ctx, h)
	return nil
}

func (o *wsOutput) Cleanup(context.Context) error {
	o.ao.Stop()
	o.w.Stop()
	return nil
}

func (o *wsOutput) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	return o.ao.Handle(f, dir, emit)
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
		if fn := t.OnDisconnect; fn != nil {
			fn()
		}
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
