package sarvam

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

const ttsURL = "https://api.sarvam.ai/text-to-speech"

// TTS calls Sarvam Bulbul REST API (linear16 PCM for the voicebot pipeline).
type TTS struct {
	name       string
	APIKey     string
	SampleRate int
	Bot        *aggregate.BotState

	mu         sync.Mutex
	canceling  bool
	textBuffer strings.Builder
}

func NewTTS(name, apiKey string, bot *aggregate.BotState, sampleRate int) *TTS {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &TTS{name: name, APIKey: apiKey, SampleRate: sampleRate, Bot: bot}
}

func (t *TTS) Name() string { return t.name }

func (t *TTS) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.LLMFullResponseStartFrame:
		t.mu.Lock()
		t.canceling = false
		t.textBuffer.Reset()
		t.mu.Unlock()
		emit.Down(f)
	case *frames.LLMFullResponseEndFrame:
		t.flushBufferedText(ctx, emit)
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

func (t *TTS) flushBufferedText(ctx context.Context, emit processor.Emit) {
	t.mu.Lock()
	text := strings.TrimSpace(t.textBuffer.String())
	t.textBuffer.Reset()
	canceling := t.canceling
	t.mu.Unlock()
	if canceling || text == "" {
		return
	}
	if strings.TrimSpace(t.APIKey) == "" {
		services.PipelineLog("tts", "sarvam: SARVAM_API_KEY is empty")
		emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("sarvam: missing API key")})
		return
	}
	preview := text
	if len(preview) > 120 {
		preview = preview[:120] + "…"
	}
	services.PipelineLog("tts", "sarvam: synthesizing %d chars, preview: %q", len(text), preview)
	pcm, sr, err := t.synth(ctx, text)
	if err != nil {
		services.PipelineLog("tts", "sarvam: %v", err)
		emit.Down(&frames.ErrorFrame{Err: err})
		return
	}
	if len(pcm) == 0 {
		services.PipelineLog("tts", "sarvam: empty PCM from API")
		return
	}
	services.PipelineLog("tts", "sarvam: synthesized %d bytes PCM (%d Hz) for %d chars", len(pcm), sr, len(text))
	if t.Bot != nil {
		t.Bot.SetSpeaking(true)
	}
	emit.Down(&frames.BotSpeakingFrame{})
	emit.Down(&frames.TTSAudioRawFrame{
		Audio:       pcm,
		SampleRate:  sr,
		NumChannels: 1,
	})
	emit.Down(&frames.BotStoppedSpeakingFrame{})
	if t.Bot != nil {
		t.Bot.SetSpeaking(false)
	}
}

type ttsRequest struct {
	Text               string `json:"text"`
	TargetLanguageCode string `json:"target_language_code"`
	Speaker            string `json:"speaker,omitempty"`
	Model              string `json:"model,omitempty"`
	SpeechSampleRate   string `json:"speech_sample_rate,omitempty"`
	OutputAudioCodec   string `json:"output_audio_codec,omitempty"`
	Pace               *float64 `json:"pace,omitempty"`
}

type ttsResponse struct {
	RequestID string   `json:"request_id"`
	Audios    []string `json:"audios"`
}

type errBody struct {
	Error struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
}

func (t *TTS) synth(ctx context.Context, text string) ([]byte, int, error) {
	srStr := sarvamSampleRateString(t.SampleRate)
	lang := strings.TrimSpace(envOr("SARVAM_LANGUAGE", "en-IN"))
	speaker := strings.TrimSpace(envOr("SARVAM_SPEAKER", "shubh"))
	model := strings.TrimSpace(envOr("SARVAM_TTS_MODEL", "bulbul:v3"))
	pace := 1.0
	if v := strings.TrimSpace(os.Getenv("SARVAM_PACE")); v != "" {
		var p float64
		if _, err := fmt.Sscanf(v, "%f", &p); err == nil && p > 0 {
			pace = p
		}
	}

	body, err := json.Marshal(ttsRequest{
		Text:               text,
		TargetLanguageCode: lang,
		Speaker:            speaker,
		Model:              model,
		SpeechSampleRate:   srStr,
		OutputAudioCodec:   "linear16",
		Pace:               &pace,
	})
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ttsURL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("api-subscription-key", t.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var eb errBody
		msg := string(raw)
		if json.Unmarshal(raw, &eb) == nil && eb.Error.Message != "" {
			msg = eb.Error.Message
		}
		return nil, 0, fmt.Errorf("sarvam: %s: %s", resp.Status, msg)
	}

	var out ttsResponse
	if json.Unmarshal(raw, &out) != nil || len(out.Audios) == 0 {
		return nil, 0, fmt.Errorf("sarvam: invalid response (no audios)")
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Audios[0])
	if err != nil {
		return nil, 0, fmt.Errorf("sarvam: decode base64: %w", err)
	}
	pcm, wavSR := rawPCMFromWAVOrLinear(decoded, t.SampleRate)
	if wavSR > 0 {
		return pcm, wavSR, nil
	}
	return pcm, t.SampleRate, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

// Sarvam accepts these sample rates as strings (bulbul:v3 REST).
func sarvamSampleRateString(sr int) string {
	switch sr {
	case 8000:
		return "8000"
	case 16000:
		return "16000"
	case 22050:
		return "22050"
	case 24000:
		return "24000"
	case 32000:
		return "32000"
	case 44100:
		return "44100"
	case 48000:
		return "48000"
	default:
		return "16000"
	}
}

// rawPCMFromWAVOrLinear strips a WAV container if present; returns PCM and sample rate from fmt chunk when found.
func rawPCMFromWAVOrLinear(b []byte, fallbackSR int) ([]byte, int) {
	if len(b) < 12 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return b, fallbackSR
	}
	off := 12
	sr := 0
	for off+8 <= len(b) {
		id := string(b[off : off+4])
		sz := int(binary.LittleEndian.Uint32(b[off+4 : off+8]))
		start := off + 8
		end := start + sz
		if end > len(b) {
			break
		}
		switch id {
		case "fmt ":
			payload := b[start:end]
			if len(payload) >= 16 {
				sr = int(binary.LittleEndian.Uint32(payload[4:8]))
			}
		case "data":
			if sr == 0 {
				sr = fallbackSR
			}
			return b[start:end], sr
		}
		off = end
		if sz%2 == 1 {
			off++
		}
	}
	return b, fallbackSR
}
