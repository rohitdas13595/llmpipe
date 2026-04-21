package google

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/internal/audiofmt"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// STT transcribes utterances with Gemini multimodal audio (Pipecat `pipecat/services/google/stt.py` uses Cloud Speech v2;
// llmpipe uses the Gemini API with the same key as google/llm.go).
type STT struct {
	name       string
	Client     *genai.Client
	Model      string
	SampleRate int
	Reenter    services.ReenterFunc
	Bot        *aggregate.BotState

	mu  sync.Mutex
	buf []byte
}

func NewSTT(name string, client *genai.Client, model string, reenter services.ReenterFunc, sampleRate int, bot *aggregate.BotState) *STT {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	if strings.TrimSpace(model) == "" {
		model = "gemini-2.0-flash"
	}
	return &STT{name: name, Client: client, Model: model, SampleRate: sampleRate, Reenter: reenter, Bot: bot}
}

func (s *STT) Name() string { return s.name }

func (s *STT) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.UserStartedSpeakingFrame:
		s.mu.Lock()
		s.buf = nil
		s.mu.Unlock()
		emit.Down(f)
	case *frames.InputAudioRawFrame:
		s.mu.Lock()
		if s.Bot == nil || !s.Bot.Speaking() {
			s.buf = append(s.buf, fr.Audio...)
		}
		s.mu.Unlock()
		emit.Down(f)
	case *frames.UserStoppedSpeakingFrame:
		emit.Down(f)
		if s.Bot != nil && s.Bot.Speaking() {
			s.mu.Lock()
			s.buf = nil
			s.mu.Unlock()
			return nil
		}
		go s.transcribe(context.Background())
	case *frames.InterimTranscriptionFrame:
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (s *STT) transcribe(ctx context.Context) {
	s.mu.Lock()
	pcm := append([]byte(nil), s.buf...)
	s.buf = nil
	s.mu.Unlock()
	if s.Reenter == nil || s.Client == nil || len(pcm) == 0 {
		return
	}
	wav := audiofmt.MonoS16LE(pcm, s.SampleRate)
	parts := []*genai.Part{{
		InlineData: &genai.Blob{MIMEType: "audio/wav", Data: wav},
	}, {
		Text: "Transcribe the speech verbatim. Output spoken words only.",
	}}
	contents := []*genai.Content{{Role: genai.RoleUser, Parts: parts}}
	cfg := &genai.GenerateContentConfig{Temperature: genai.Ptr(float32(0))}
	res, err := s.Client.Models.GenerateContent(ctx, s.Model, contents, cfg)
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	text := strings.TrimSpace(res.Text())
	if text == "" {
		return
	}
	services.PipelineLog("stt", "google (Gemini audio): %q", text)
	_ = s.Reenter(ctx, s.name, &frames.TranscriptionFrame{Text: text})
}
