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
	rtwire "github.com/rohitdas13595/llmpipe/adapters/realtime"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/internal/resample"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/llmbase"
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
	tools      *llmbase.Bag
	endpoint   string
	fns        llmbase.CancelGroup
	fnGen      atomic.Uint64
	pending    map[string]*pendingFn
	fnOrder    []string
}

type pendingFn struct {
	id, name, args string
	delta          strings.Builder
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
	return &Realtime{name: name, APIKey: apiKey, Model: model, SystemPrompt: system, Reenter: re, Bot: bot, tools: &llmbase.Bag{}}
}

// SetRegistry installs function handlers. They are sent in session.update.
func (r *Realtime) SetRegistry(reg *tools.Registry) {
	if r.tools == nil {
		r.tools = &llmbase.Bag{}
	}
	r.tools.SetRegistry(reg)
	r.sendSession()
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
		pcm := fr.Audio
		rate := fr.SampleRate
		if rate <= 0 {
			rate = r.inRate
		}
		if rate != openAIRealtimeInputRate {
			pcm = resample.LinearS16LE(pcm, rate, openAIRealtimeInputRate)
		}
		r.sendJSON(map[string]any{
			"type":  "input_audio_buffer.append",
			"audio": base64.StdEncoding.EncodeToString(pcm),
		})
	case *frames.InterruptionFrame:
		r.fnGen.Add(1)
		r.fns.Cancel()
		r.clearPending()
		r.cancelResponse()
		processor.Forward(dir, f, emit)
	case *frames.LLMSetToolsFrame:
		if r.tools != nil {
			r.tools.SetSchema(fr.Tools)
		}
		r.sendSession()
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
	defer r.mu.Unlock()
	if r.conn == nil {
		return
	}
	_ = r.conn.WriteJSON(v)
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
	rawURL := r.endpoint
	if rawURL == "" {
		u, err := url.Parse("wss://api.openai.com/v1/realtime")
		if err != nil {
			return
		}
		q := u.Query()
		q.Set("model", r.Model)
		u.RawQuery = q.Encode()
		rawURL = u.String()
	}

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+r.APIKey)
	hdr.Set("OpenAI-Beta", "realtime=v1")

	ctx, cancel := context.WithCancel(bg)
	d := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	conn, resp, err := d.DialContext(ctx, rawURL, hdr)
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

	r.sendSession()

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

func (r *Realtime) sendSession() {
	sessMap := map[string]any{
		"modalities":          []string{"text", "audio"},
		"input_audio_format":  "pcm16",
		"output_audio_format": "pcm16",
	}
	if sys := strings.TrimSpace(r.SystemPrompt); sys != "" {
		sessMap["instructions"] = sys
	}
	if r.tools != nil {
		schema := r.tools.Schema()
		if len(schema.Functions) > 0 {
			wire, err := (rtwire.Session{}).Tools(schema)
			if err == nil {
				sessMap["tools"] = wire
				sessMap["tool_choice"] = "auto"
			}
		}
	}
	r.sendJSON(map[string]any{"type": "session.update", "session": sessMap})
}

func (r *Realtime) clearPending() {
	r.mu.Lock()
	r.pending = nil
	r.fnOrder = nil
	r.mu.Unlock()
}

func (r *Realtime) noteFn(id, name, args, delta string, done bool) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]*pendingFn{}
	}
	p := r.pending[id]
	if p == nil {
		p = &pendingFn{id: id}
		r.pending[id] = p
		r.fnOrder = append(r.fnOrder, id)
	}
	if name != "" {
		p.name = name
	}
	if delta != "" {
		p.delta.WriteString(delta)
	}
	if done {
		if args != "" {
			p.args = args
		} else if p.args == "" {
			p.args = p.delta.String()
		}
	}
}

func (r *Realtime) takePending() []aggregate.ToolCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fnOrder) == 0 {
		return nil
	}
	out := make([]aggregate.ToolCall, 0, len(r.fnOrder))
	for _, id := range r.fnOrder {
		p := r.pending[id]
		if p == nil {
			continue
		}
		args := p.args
		if args == "" {
			args = p.delta.String()
		}
		if args == "" {
			args = "{}"
		}
		out = append(out, aggregate.ToolCall{ID: p.id, Name: p.name, Args: json.RawMessage(args)})
	}
	r.pending = nil
	r.fnOrder = nil
	return out
}

func (r *Realtime) flushFns() {
	calls := r.takePending()
	if len(calls) == 0 || r.tools == nil || r.tools.Registry() == nil {
		return
	}
	gen := r.fnGen.Load()
	parent := r.ProcContext()
	turn := r.fns.Replace(parent)
	go llmbase.Run(turn, parent, r.tools, nil, "", calls, llmbase.Hooks{
		Down: func(ctx context.Context, f frames.Frame) error { return r.push(ctx, f) },
		Up: func(ctx context.Context, f frames.Frame) error {
			if r.H() == nil {
				return nil
			}
			return services.PushUp(ctx, r.H(), r.name, f)
		},
		OnResult: func(call aggregate.ToolCall, res tools.Result, cancelled bool) {
			if cancelled || r.fnGen.Load() != gen {
				return
			}
			r.sendJSON(rtwire.FunctionOutput(call.ID, res.Content))
		},
		OnBatchDone: func(cancelled bool) {
			if cancelled || r.fnGen.Load() != gen {
				return
			}
			r.sendJSON(rtwire.ResponseCreate())
		},
	})
}

func (r *Realtime) handleServer(ctx context.Context, data []byte) {
	var ev struct {
		Type      string `json:"type"`
		Delta     string `json:"delta"`
		ItemID    string `json:"item_id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return
	}
	switch ev.Type {
	case "response.function_call_arguments.delta":
		r.noteFn(ev.CallID, ev.Name, "", ev.Delta, false)
		return
	case "response.function_call_arguments.done":
		r.noteFn(ev.CallID, ev.Name, ev.Arguments, "", true)
		return
	case "response.done", "response.completed":
		r.flushFns()
		return
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
