// Realtime WebSocket session (Pipecat: pipecat/services/openai/realtime/llm.py).
package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/internal/resample"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

const openAIRealtimeInputRate = 24000

// Realtime streams audio to OpenAI Realtime and emits output audio frames (pcm16 @ 24 kHz).
type Realtime struct {
	name         string
	APIKey       string
	Model        string
	SystemPrompt string
	Reenter      services.ReenterFunc
	Bot          *aggregate.BotState

	services.Slot
	started    atomic.Bool
	mu         sync.Mutex
	conn       realtimeSocket
	recvCancel context.CancelFunc
	inRate     int
	voiceOut   bool
	itemID     string
}

// realtimeSocket is the subset of *websocket.Conn the session uses.
type realtimeSocket interface {
	WriteJSON(v any) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

func NewRealtime(name, apiKey, model, system string, re services.ReenterFunc, bot *aggregate.BotState) *Realtime {
	if strings.TrimSpace(model) == "" {
		model = os.Getenv("OPENAI_REALTIME_MODEL")
		if model == "" {
			model = "gpt-4o-realtime-preview-2024-12-17"
		}
	}
	return &Realtime{name: name, APIKey: apiKey, Model: model, SystemPrompt: system, Reenter: re, Bot: bot}
}

func (r *Realtime) Name() string { return r.name }

func (r *Realtime) push(ctx context.Context, f frames.Frame) error {
	return services.PushDown(ctx, r.H(), r.Reenter, r.name, f)
}

// Setup stores the pipeline handle and starts the realtime socket on the
// processor context. The dial itself runs in the background so Setup does
// not block the rest of the pipeline on the network.
func (r *Realtime) Setup(ctx context.Context, h processor.Handle) error {
	if err := r.Slot.Setup(ctx, h); err != nil {
		return err
	}
	r.ensure(h.Context())
	return nil
}

// Cleanup closes the realtime socket.
func (r *Realtime) Cleanup(ctx context.Context) error {
	r.closeConn()
	return nil
}

func (r *Realtime) ensure(ctx context.Context) {
	if strings.TrimSpace(r.APIKey) == "" || (r.H() == nil && r.Reenter == nil) {
		return
	}
	if !r.started.CompareAndSwap(false, true) {
		return
	}
	go r.connect(ctx)
}

func (r *Realtime) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.BotStartedSpeakingFrame, *frames.BotSpeakingFrame:
		if r.Bot != nil {
			r.Bot.SetSpeaking(true)
		}
		processor.Forward(dir, f, emit)
		return nil
	case *frames.BotStoppedSpeakingFrame:
		if r.Bot != nil {
			r.Bot.SetSpeaking(false)
		}
		if fr.Interrupted {
			r.truncate(fr.PlayedMS)
		}
		processor.Forward(dir, f, emit)
		return nil
	}
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
	case *frames.StartFrame:
		r.inRate = fr.SampleRate
		if r.inRate <= 0 {
			r.inRate = 16000
		}
		emit.Down(f)
		parent := context.Background()
		if h := r.H(); h != nil {
			parent = h.Context()
		}
		r.ensure(parent)
	case *frames.InputAudioRawFrame:
		emit.Down(f)
		r.mu.Lock()
		c := r.conn
		r.mu.Unlock()
		if c == nil {
			return nil
		}
		pcm := fr.Audio
		rate := fr.SampleRate
		if rate <= 0 {
			rate = r.inRate
		}
		if rate != openAIRealtimeInputRate {
			pcm = resample.LinearS16LE(pcm, rate, openAIRealtimeInputRate)
		}
		b64 := base64.StdEncoding.EncodeToString(pcm)
		msg := map[string]any{
			"type":  "input_audio_buffer.append",
			"audio": b64,
		}
		_ = c.WriteJSON(msg)
	case *frames.InterruptionFrame:
		r.cancelResponse()
		processor.Forward(dir, f, emit)
	case *frames.CancelFrame, *frames.EndFrame:
		r.closeConn()
		processor.Forward(dir, f, emit)
	default:
		processor.Forward(dir, f, emit)
	}
	return nil
}

func (r *Realtime) sendJSON(v any) {
	r.mu.Lock()
	c := r.conn
	r.mu.Unlock()
	if c == nil {
		return
	}
	_ = c.WriteJSON(v)
}

func (r *Realtime) cancelResponse() {
	r.sendJSON(map[string]any{"type": "response.cancel"})
}

func (r *Realtime) truncate(playedMS int) {
	r.mu.Lock()
	id := r.itemID
	r.mu.Unlock()
	if id == "" {
		return
	}
	if playedMS < 0 {
		playedMS = 0
	}
	r.sendJSON(map[string]any{
		"type":          "conversation.item.truncate",
		"item_id":       id,
		"content_index": 0,
		"audio_end_ms":  playedMS,
	})
}

func (r *Realtime) connect(bg context.Context) {
	if strings.TrimSpace(r.APIKey) == "" || (r.H() == nil && r.Reenter == nil) {
		return
	}
	u, err := url.Parse("wss://api.openai.com/v1/realtime")
	if err != nil {
		return
	}
	q := u.Query()
	q.Set("model", r.Model)
	u.RawQuery = q.Encode()

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+r.APIKey)
	hdr.Set("OpenAI-Beta", "realtime=v1")

	ctx, cancel := context.WithCancel(bg)
	d := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	conn, resp, err := d.DialContext(ctx, u.String(), hdr)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		cancel()
		r.started.Store(false)
		services.PipelineLog("realtime", "openai dial: %v", err)
		_ = r.push(bg, &frames.ErrorFrame{Err: err})
		return
	}
	r.mu.Lock()
	r.conn = conn
	r.recvCancel = cancel
	r.mu.Unlock()

	sessMap := map[string]any{
		"modalities":          []string{"text", "audio"},
		"input_audio_format":  "pcm16",
		"output_audio_format": "pcm16",
	}
	if sys := strings.TrimSpace(r.SystemPrompt); sys != "" {
		sessMap["instructions"] = sys
	}
	_ = conn.WriteJSON(map[string]any{"type": "session.update", "session": sessMap})

	go r.readLoop(ctx, conn)
}

func (r *Realtime) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.handleServer(ctx, data)
	}
}

func (r *Realtime) handleServer(ctx context.Context, data []byte) {
	var ev struct {
		Type   string `json:"type"`
		Delta  string `json:"delta"`
		ItemID string `json:"item_id"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return
	}
	switch ev.Type {
	case "response.output_audio.delta", "response.audio.delta":
		if ev.ItemID != "" {
			r.mu.Lock()
			r.itemID = ev.ItemID
			r.mu.Unlock()
		}
		raw, err := base64.StdEncoding.DecodeString(ev.Delta)
		if err != nil || len(raw) == 0 {
			return
		}
		r.mu.Lock()
		first := !r.voiceOut
		r.voiceOut = true
		r.mu.Unlock()
		if first {
			_ = r.push(ctx, &frames.TTSStartedFrame{})
		}
		_ = r.push(ctx, &frames.TTSAudioRawFrame{
			Audio: raw, SampleRate: openAIRealtimeInputRate, NumChannels: 1,
		})
	case "response.output_audio.done", "response.audio.done":
		r.mu.Lock()
		r.voiceOut = false
		r.mu.Unlock()
		_ = r.push(ctx, &frames.TTSStoppedFrame{})
	}
}

func (r *Realtime) closeConn() {
	r.mu.Lock()
	if r.recvCancel != nil {
		r.recvCancel()
		r.recvCancel = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.mu.Unlock()
}
