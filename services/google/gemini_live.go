// Gemini Live API processor (Pipecat: pipecat/services/google/gemini_live/llm.py).
package google

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// GeminiLive streams bidirectional audio over the Gemini Multimodal Live WebSocket (google.genai Live API).
type GeminiLive struct {
	name         string
	Client       *genai.Client
	Model        string
	SystemPrompt string
	Reenter      services.ReenterFunc
	Bot          *aggregate.BotState

	mu          sync.Mutex
	session     *genai.Session
	recvCancel  context.CancelFunc
	sampleRate  int
	connecting  bool
	connectErr  error
}

func NewGeminiLive(name string, client *genai.Client, model, system string, re services.ReenterFunc, bot *aggregate.BotState) *GeminiLive {
	if strings.TrimSpace(model) == "" {
		model = "gemini-2.0-flash-live-preview-04-09"
	}
	return &GeminiLive{
		name: name, Client: client, Model: model, SystemPrompt: system, Reenter: re, Bot: bot,
	}
}

func (g *GeminiLive) Name() string { return g.name }

func (g *GeminiLive) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.StartFrame:
		g.sampleRate = fr.SampleRate
		if g.sampleRate <= 0 {
			g.sampleRate = 16000
		}
		emit.Down(f)
		go g.connect(context.Background())
	case *frames.InputAudioRawFrame:
		emit.Down(f)
		g.mu.Lock()
		sess := g.session
		g.mu.Unlock()
		if sess == nil {
			return nil
		}
		mime := "audio/pcm;rate=" + strconv.Itoa(fr.SampleRate)
		if fr.SampleRate <= 0 {
			mime = "audio/pcm;rate=16000"
		}
		_ = sess.SendRealtimeInput(genai.LiveRealtimeInput{
			Audio: &genai.Blob{MIMEType: mime, Data: fr.Audio},
		})
	case *frames.InterruptionFrame, *frames.CancelFrame, *frames.EndFrame:
		g.shutdown()
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (g *GeminiLive) connect(bg context.Context) {
	g.mu.Lock()
	if g.connecting || g.session != nil {
		g.mu.Unlock()
		return
	}
	g.connecting = true
	g.mu.Unlock()

	if g.Client == nil || g.Reenter == nil {
		return
	}
	cfg := &genai.LiveConnectConfig{
		ResponseModalities: []genai.Modality{genai.ModalityAudio, genai.ModalityText},
	}
	if strings.TrimSpace(g.SystemPrompt) != "" {
		cfg.SystemInstruction = &genai.Content{Parts: []*genai.Part{{Text: g.SystemPrompt}}}
	}
	ctx, cancel := context.WithCancel(bg)
	sess, err := g.Client.Live.Connect(ctx, g.Model, cfg)
	g.mu.Lock()
	g.connecting = false
	g.connectErr = err
	if err == nil {
		g.session = sess
		g.recvCancel = cancel
	} else {
		cancel()
		g.recvCancel = nil
	}
	g.mu.Unlock()
	if err != nil {
		services.PipelineLog("realtime", "gemini live connect: %v", err)
		_ = g.Reenter(bg, g.name, &frames.ErrorFrame{Err: err})
		return
	}
	services.PipelineLog("realtime", "gemini live: connected model=%q", g.Model)

	go g.receiveLoop(ctx, sess)
}

func (g *GeminiLive) receiveLoop(ctx context.Context, sess *genai.Session) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := sess.Receive()
		if err != nil {
			services.PipelineLog("realtime", "gemini live receive: %v", err)
			return
		}
		if msg == nil {
			continue
		}
		if msg.ServerContent != nil && msg.ServerContent.Interrupted && g.Reenter != nil {
			_ = g.Reenter(ctx, g.name, &frames.InterruptionFrame{})
		}
		if msg.ServerContent != nil && msg.ServerContent.ModelTurn != nil {
			for _, p := range msg.ServerContent.ModelTurn.Parts {
				if p == nil || p.InlineData == nil {
					continue
				}
				d := p.InlineData.Data
				if len(d) == 0 {
					continue
				}
				mime := p.InlineData.MIMEType
				rate := 24000
				if i := strings.Index(mime, "rate="); i >= 0 {
					if n, err := strconv.Atoi(strings.TrimSpace(mime[i+5:])); err == nil && n > 0 {
						rate = n
					}
				}
				if g.Bot != nil {
					g.Bot.SetSpeaking(true)
				}
				if g.Reenter != nil {
					_ = g.Reenter(ctx, g.name, &frames.BotSpeakingFrame{})
					_ = g.Reenter(ctx, g.name, &frames.TTSAudioRawFrame{Audio: d, SampleRate: rate, NumChannels: 1})
					_ = g.Reenter(ctx, g.name, &frames.BotStoppedSpeakingFrame{})
				}
				if g.Bot != nil {
					g.Bot.SetSpeaking(false)
				}
			}
		}
	}
}

func (g *GeminiLive) shutdown() {
	g.mu.Lock()
	if g.recvCancel != nil {
		g.recvCancel()
		g.recvCancel = nil
	}
	if g.session != nil {
		_ = g.session.Close()
		g.session = nil
	}
	g.mu.Unlock()
}
