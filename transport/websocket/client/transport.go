// Package client implements an outbound WebSocket client with the same PCM/control framing as transport/ws.
package client

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

// Transport mirrors transport/ws but initiates the WebSocket as a client.
type Transport struct {
	SampleRate int
	// Dialer defaults to websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// Header is optional extra headers (Authorization, etc.).
	Header http.Header
	// URL is the WebSocket URI (ws:// or wss://).
	URL string

	queue func(context.Context, []frames.Frame) error
	mu    sync.RWMutex
	conn  *websocket.Conn
	// OnDisconnect runs when the connection closes or read fails.
	OnDisconnect func()
}

// NewTransport builds a client transport. queue should inject at pipeline start (e.g. task.QueueFrames).
func NewTransport(sampleRate int, url string, queue func(context.Context, []frames.Frame) error) *Transport {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &Transport{SampleRate: sampleRate, URL: url, queue: queue, Dialer: websocket.DefaultDialer}
}

func (t *Transport) setConn(c *websocket.Conn) {
	t.mu.Lock()
	t.conn = c
	t.mu.Unlock()
}

// Connect dials the server and starts reading messages into the pipeline until ctx is done.
// It queues StartFrame then spawns a read loop. Safe to call once per pipeline run.
func (t *Transport) Connect(ctx context.Context) error {
	d := t.Dialer
	if d == nil {
		d = websocket.DefaultDialer
	}
	c, _, err := d.DialContext(ctx, t.URL, t.Header)
	if err != nil {
		return err
	}
	t.setConn(c)
	if t.queue != nil {
		_ = t.queue(ctx, []frames.Frame{
			&frames.StartFrame{SampleRate: t.SampleRate, NumChannels: 1},
		})
	}
	go t.readLoop(ctx, c)
	return nil
}

func (t *Transport) readLoop(ctx context.Context, c *websocket.Conn) {
	defer func() {
		_ = c.Close()
		t.setConn(nil)
		if fn := t.OnDisconnect; fn != nil {
			fn()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		mt, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.TextMessage && t.queue != nil {
			if handleControl(data, t.queue) {
				continue
			}
		}
		if mt != websocket.BinaryMessage || len(data) == 0 || t.queue == nil {
			continue
		}
		_ = t.queue(context.Background(), []frames.Frame{
			&frames.InputAudioRawFrame{
				Audio:       data,
				SampleRate:  t.SampleRate,
				NumChannels: 1,
			},
		})
	}
}

// Close closes the active connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	c := t.conn
	t.conn = nil
	t.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

// Input is a passthrough processor (audio enters via queue from the read loop).
func (t *Transport) Input() processor.Processor {
	return processor.Func{
		N: "websocket.client.input",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			emit.Down(f)
			return nil
		},
	}
}

// Output writes TTSAudioRawFrame to the outbound WebSocket as binary messages.
func (t *Transport) Output() processor.Processor {
	return &wsClientOut{t: t}
}

type wsClientOut struct {
	t *Transport
}

func (o *wsClientOut) Name() string { return "websocket.client.output" }

func (o *wsClientOut) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if a, ok := f.(*frames.TTSAudioRawFrame); ok {
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

type ctlJSON struct {
	EndUtterance bool `json:"endUtterance"`
}

func handleControl(data []byte, queue func(context.Context, []frames.Frame) error) bool {
	s := strings.TrimSpace(string(data))
	if s == "" {
		return false
	}
	var c ctlJSON
	if json.Unmarshal(data, &c) == nil && c.EndUtterance {
		_ = queue(context.Background(), []frames.Frame{&frames.UserStoppedSpeakingFrame{}})
		return true
	}
	if strings.EqualFold(s, "end") || strings.EqualFold(s, "end_utterance") {
		_ = queue(context.Background(), []frames.Frame{&frames.UserStoppedSpeakingFrame{}})
		return true
	}
	return false
}
