package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/internal/resample"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// TTSService streams synthesized audio via OpenAI /v1/audio/speech (Pipecat: OpenAITTSService, 24 kHz PCM).
type TTSService struct {
	name       string
	APIKey     string
	BaseURL    string
	Model      string
	Voice      string
	SampleRate int // pipeline / output rate (resampled from 24000)
	Bot        *aggregate.BotState

	mu         sync.Mutex
	canceling  bool
	textBuffer strings.Builder
}

func NewTTSService(name, apiKey, voice string, bot *aggregate.BotState, pipelineSampleRate int) *TTSService {
	if pipelineSampleRate <= 0 {
		pipelineSampleRate = 16000
	}
	base := os.Getenv("OPENAI_BASE_URL")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	model := os.Getenv("OPENAI_TTS_MODEL")
	if model == "" {
		model = "tts-1"
	}
	if voice == "" {
		voice = "alloy"
	}
	return &TTSService{
		name:       name,
		APIKey:     apiKey,
		BaseURL:    strings.TrimSuffix(base, "/"),
		Model:      model,
		Voice:      voice,
		SampleRate: pipelineSampleRate,
		Bot:        bot,
	}
}

// NewTTSServiceWithBaseURL is like NewTTSService but fixes BaseURL, model, and voice (Pipecat-style per-provider TTS).
func NewTTSServiceWithBaseURL(name, apiKey, baseURL, model, voice string, bot *aggregate.BotState, pipelineSampleRate int) *TTSService {
	if pipelineSampleRate <= 0 {
		pipelineSampleRate = 16000
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	if model == "" {
		model = "tts-1"
	}
	if voice == "" {
		voice = "alloy"
	}
	return &TTSService{
		name:       name,
		APIKey:     apiKey,
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		Model:      model,
		Voice:      voice,
		SampleRate: pipelineSampleRate,
		Bot:        bot,
	}
}

func (t *TTSService) Name() string { return t.name }

func (t *TTSService) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.LLMFullResponseStartFrame:
		t.mu.Lock()
		t.canceling = false
		t.textBuffer.Reset()
		t.mu.Unlock()
		emit.Down(f)
	case *frames.LLMFullResponseEndFrame:
		t.flush(ctx, emit)
		emit.Down(f)
	case *frames.InterruptionFrame:
		t.mu.Lock()
		t.canceling = true
		t.textBuffer.Reset()
		t.mu.Unlock()
		emit.Down(f)
	case *frames.LLMTextFrame:
		t.mu.Lock()
		t.textBuffer.WriteString(fr.Text)
		t.mu.Unlock()
		emit.Down(f)
	case *frames.TextFrame:
		t.mu.Lock()
		t.textBuffer.WriteString(fr.Text)
		t.mu.Unlock()
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

type speechReq struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	ResponseFormat string `json:"response_format"`
}

const openAITTSRate = 24000

func (t *TTSService) flush(ctx context.Context, emit processor.Emit) {
	t.mu.Lock()
	text := strings.TrimSpace(t.textBuffer.String())
	t.textBuffer.Reset()
	canceling := t.canceling
	t.mu.Unlock()
	if canceling || text == "" {
		return
	}
	if t.APIKey == "" {
		emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("openai tts: missing API key")})
		return
	}
	body, _ := json.Marshal(speechReq{
		Model:          t.Model,
		Input:          text,
		Voice:          t.Voice,
		ResponseFormat: "pcm",
	})
	base := t.BaseURL
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	u := base + "/audio/speech"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		emit.Down(&frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("Authorization", "Bearer "+t.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		emit.Down(&frames.ErrorFrame{Err: err})
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("openai tts: %s: %s", resp.Status, string(raw))})
		return
	}
	pcm := raw
	if t.SampleRate != openAITTSRate {
		pcm = resample.LinearS16LE(pcm, openAITTSRate, t.SampleRate)
	}
	services.PipelineLog("tts", "openai: pcm %d bytes → %d Hz", len(pcm), t.SampleRate)
	if t.Bot != nil {
		t.Bot.SetSpeaking(true)
	}
	emit.Down(&frames.BotSpeakingFrame{})
	emit.Down(&frames.TTSAudioRawFrame{Audio: pcm, SampleRate: t.SampleRate, NumChannels: 1})
	emit.Down(&frames.BotStoppedSpeakingFrame{})
	if t.Bot != nil {
		t.Bot.SetSpeaking(false)
	}
}
