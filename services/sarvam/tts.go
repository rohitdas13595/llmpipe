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

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/services/ttsbase"
)

const ttsURL = "https://api.sarvam.ai/text-to-speech"

// NewTTS calls Sarvam Bulbul over REST, one request per sentence.
// bot is accepted for compatibility; playback state comes from the output.
func NewTTS(name, apiKey string, bot *aggregate.BotState, sampleRate int) *ttsbase.TTS {
	_ = bot
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return ttsbase.New(&restSynth{apiKey: apiKey, sampleRate: sampleRate}, ttsbase.Options{
		Name:           name,
		SampleRate:     sampleRate,
		PushTextFrames: true,
	})
}

type restSynth struct {
	apiKey     string
	sampleRate int
}

func (t *restSynth) Synthesize(ctx context.Context, text string, out func(ttsbase.AudioChunk) error) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if strings.TrimSpace(t.apiKey) == "" {
		return fmt.Errorf("sarvam: missing API key")
	}
	pcm, sr, err := t.synth(ctx, text)
	if err != nil {
		return err
	}
	if len(pcm) == 0 {
		return nil
	}
	return out(ttsbase.AudioChunk{PCM: pcm, SampleRate: sr})
}

type ttsRequest struct {
	Text               string   `json:"text"`
	TargetLanguageCode string   `json:"target_language_code"`
	Speaker            string   `json:"speaker,omitempty"`
	Model              string   `json:"model,omitempty"`
	SpeechSampleRate   string   `json:"speech_sample_rate,omitempty"`
	OutputAudioCodec   string   `json:"output_audio_codec,omitempty"`
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

func (t *restSynth) synth(ctx context.Context, text string) ([]byte, int, error) {
	srStr := sarvamSampleRateString(t.sampleRate)
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
	req.Header.Set("api-subscription-key", t.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
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
	pcm, wavSR := rawPCMFromWAVOrLinear(decoded, t.sampleRate)
	if wavSR > 0 {
		return pcm, wavSR, nil
	}
	return pcm, t.sampleRate, nil
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
