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

	mu         sync.Mutex
	conn       *websocket.Conn
	recvCancel context.CancelFunc
	inRate     int
	voiceOut   bool // whether we already emitted BotSpeakingFrame for this turn
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

func (r *Realtime) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.StartFrame:
		r.inRate = fr.SampleRate
		if r.inRate <= 0 {
			r.inRate = 16000
		}
		emit.Down(f)
		go r.connect(context.Background())
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
	case *frames.InterruptionFrame, *frames.CancelFrame, *frames.EndFrame:
		r.closeConn()
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (r *Realtime) connect(bg context.Context) {
	if strings.TrimSpace(r.APIKey) == "" || r.Reenter == nil {
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
	conn, _, err := d.DialContext(ctx, u.String(), hdr)
	if err != nil {
		cancel()
		services.PipelineLog("realtime", "openai dial: %v", err)
		_ = r.Reenter(bg, r.name, &frames.ErrorFrame{Err: err})
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
		var ev map[string]json.RawMessage
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		tbytes, ok := ev["type"]
		if !ok {
			continue
		}
		var typ string
		if json.Unmarshal(tbytes, &typ) != nil {
			continue
		}
		if typ == "response.output_audio.delta" {
			var delta string
			if json.Unmarshal(ev["delta"], &delta) != nil {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(delta)
			if err != nil || len(raw) == 0 {
				continue
			}
			r.mu.Lock()
			first := !r.voiceOut
			if first {
				r.voiceOut = true
			}
			r.mu.Unlock()
			if r.Bot != nil {
				r.Bot.SetSpeaking(true)
			}
			if r.Reenter != nil {
				if first {
					_ = r.Reenter(ctx, r.name, &frames.BotSpeakingFrame{})
				}
				_ = r.Reenter(ctx, r.name, &frames.TTSAudioRawFrame{
					Audio: raw, SampleRate: openAIRealtimeInputRate, NumChannels: 1,
				})
			}
		}
		if typ == "response.output_audio.done" {
			r.mu.Lock()
			r.voiceOut = false
			r.mu.Unlock()
			if r.Reenter != nil {
				_ = r.Reenter(ctx, r.name, &frames.BotStoppedSpeakingFrame{})
			}
			if r.Bot != nil {
				r.Bot.SetSpeaking(false)
			}
		}
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
