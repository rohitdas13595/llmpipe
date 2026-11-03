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

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/internal/resample"
	"github.com/rohitdas13595/llmpipe/services/ttsbase"
)

// TTSService synthesizes with OpenAI /v1/audio/speech (24 kHz PCM, resampled
// to the pipeline rate) and speaks one sentence at a time.
type TTSService struct {
	*ttsbase.TTS
}

func NewTTSService(name, apiKey, voice string, bot *aggregate.BotState, pipelineSampleRate int) *TTSService {
	return NewTTSServiceWithBaseURL(name, apiKey, "", "", voice, bot, pipelineSampleRate)
}

// NewTTSServiceWithBaseURL fixes the base URL, model, and voice (Groq and other
// OpenAI-compatible speech endpoints).
func NewTTSServiceWithBaseURL(name, apiKey, baseURL, model, voice string, bot *aggregate.BotState, pipelineSampleRate int) *TTSService {
	_ = bot
	if pipelineSampleRate <= 0 {
		pipelineSampleRate = 16000
	}
	if baseURL == "" {
		baseURL = os.Getenv("OPENAI_BASE_URL")
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	if model == "" {
		model = os.Getenv("OPENAI_TTS_MODEL")
	}
	if model == "" {
		model = "tts-1"
	}
	if voice == "" {
		voice = "alloy"
	}
	synth := &restSynth{
		apiKey:     apiKey,
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		model:      model,
		voice:      voice,
		sampleRate: pipelineSampleRate,
	}
	return &TTSService{TTS: ttsbase.New(synth, ttsbase.Options{
		Name:           name,
		SampleRate:     pipelineSampleRate,
		PushTextFrames: true,
	})}
}

type restSynth struct {
	apiKey     string
	baseURL    string
	model      string
	voice      string
	sampleRate int
}

type speechReq struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	ResponseFormat string `json:"response_format"`
}

const openAITTSRate = 24000

func (t *restSynth) Synthesize(ctx context.Context, text string, out func(ttsbase.AudioChunk) error) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if t.apiKey == "" {
		return fmt.Errorf("openai tts: missing API key")
	}
	body, err := json.Marshal(speechReq{
		Model:          t.model,
		Input:          text,
		Voice:          t.voice,
		ResponseFormat: "pcm",
	})
	if err != nil {
		return err
	}
	base := t.baseURL
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/audio/speech", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("openai tts: %s: %s", resp.Status, string(raw))
	}
	pcm := raw
	sr := t.sampleRate
	if sr <= 0 {
		sr = 16000
	}
	if sr != openAITTSRate {
		pcm = resample.LinearS16LE(pcm, openAITTSRate, sr)
	}
	if len(pcm) == 0 {
		return nil
	}
	return out(ttsbase.AudioChunk{PCM: pcm, SampleRate: sr})
}
