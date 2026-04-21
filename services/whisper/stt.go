// Package whisper implements OpenAI-compatible /v1/audio/transcriptions STT (Whisper),
// matching Pipecat's BaseWhisperSTTService / OpenAISTTService / GroqSTTService pattern.
package whisper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/internal/audiofmt"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// Config holds OpenAI-compatible Whisper API settings (Pipecat: base_url + model + api_key).
type Config struct {
	BaseURL    string // e.g. https://api.openai.com/v1 or https://api.groq.com/openai/v1
	APIKey     string
	Model      string
	Language   string // optional ISO-639-1, e.g. en
	SampleRate int    // input PCM rate (mono s16le)
}

// STT buffers utterances (VAD) and POSTs WAV to the transcriptions endpoint.
type STT struct {
	name    string
	cfg     Config
	Reenter services.ReenterFunc
	Bot     *aggregate.BotState

	mu  sync.Mutex
	buf bytes.Buffer
}

func NewSTT(name string, cfg Config, reenter services.ReenterFunc, sampleRate int, bot *aggregate.BotState) *STT {
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = sampleRate
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 16000
	}
	return &STT{name: name, cfg: cfg, Reenter: reenter, Bot: bot}
}

func (s *STT) Name() string { return s.name }

func (s *STT) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.UserStartedSpeakingFrame:
		s.mu.Lock()
		s.buf.Reset()
		s.mu.Unlock()
		emit.Down(f)
	case *frames.InputAudioRawFrame:
		s.mu.Lock()
		if s.Bot == nil || !s.Bot.Speaking() {
			s.buf.Write(fr.Audio)
		}
		s.mu.Unlock()
		emit.Down(f)
	case *frames.UserStoppedSpeakingFrame:
		emit.Down(f)
		if s.Bot != nil && s.Bot.Speaking() {
			s.mu.Lock()
			s.buf.Reset()
			s.mu.Unlock()
			return nil
		}
		go s.flush(context.Background())
	case *frames.InterimTranscriptionFrame:
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

type transcribeResp struct {
	Text string `json:"text"`
}

func (s *STT) flush(ctx context.Context) {
	s.mu.Lock()
	audio := append([]byte(nil), s.buf.Bytes()...)
	s.buf.Reset()
	s.mu.Unlock()
	if s.Reenter == nil {
		return
	}
	if strings.TrimSpace(s.cfg.APIKey) == "" {
		services.PipelineLog("stt", "whisper: API key empty (OPENAI_API_KEY or GROQ_API_KEY)")
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: fmt.Errorf("whisper: missing API key")})
		return
	}
	if len(audio) == 0 {
		return
	}
	if !shouldSend(audio, s.cfg.SampleRate) {
		return
	}
	base := strings.TrimSuffix(strings.TrimSpace(s.cfg.BaseURL), "/")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	model := strings.TrimSpace(s.cfg.Model)
	if model == "" {
		model = "whisper-1"
	}
	wav := audiofmt.MonoS16LE(audio, s.cfg.SampleRate)
	u := base + "/audio/transcriptions"

	var body bytes.Buffer
	mp := multipart.NewWriter(&body)
	_ = mp.WriteField("model", model)
	if s.cfg.Language != "" {
		_ = mp.WriteField("language", s.cfg.Language)
	}
	fw, err := mp.CreateFormFile("file", "audio.wav")
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	_, _ = fw.Write(wav)
	_ = mp.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, &body)
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.APIKey)
	req.Header.Set("Content-Type", mp.FormDataContentType())

	services.PipelineLog("stt", "whisper: POST %s model=%q bytes=%d", u, model, len(wav))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		services.PipelineLog("stt", "whisper: %v", err)
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: fmt.Errorf("whisper: %s: %s", resp.Status, string(raw))})
		return
	}
	var out transcribeResp
	if json.Unmarshal(raw, &out) != nil || strings.TrimSpace(out.Text) == "" {
		services.PipelineLog("stt", "whisper: bad response: %q", string(raw))
		return
	}
	services.PipelineLog("stt", "whisper transcript: %q", out.Text)
	_ = s.Reenter(ctx, s.name, &frames.TranscriptionFrame{Text: strings.TrimSpace(out.Text)})
}

func pcmRMS16LE(pcm []byte) float64 {
	if len(pcm) < 2 {
		return 0
	}
	var sum float64
	for i := 0; i+1 < len(pcm); i += 2 {
		v := int16(binary.LittleEndian.Uint16(pcm[i : i+2]))
		sum += float64(v) * float64(v)
	}
	n := float64(len(pcm) / 2)
	return math.Sqrt(sum / n)
}

func shouldSend(pcm []byte, sampleRate int) bool {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	minBytes := sampleRate * 2 / 10
	if minBytes < 3200 {
		minBytes = 3200
	}
	if v := os.Getenv("STT_MIN_PCM_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			minBytes = n
		}
	}
	if len(pcm) < minBytes {
		return false
	}
	minRMS := 55.0
	if v := os.Getenv("STT_MIN_RMS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			minRMS = f
		}
	}
	return pcmRMS16LE(pcm) >= minRMS
}

// ResolveBaseURL returns OpenAI API base (Pipecat: OPENAI base_url / env OPENAI_BASE_URL).
func ResolveBaseURL(defaultBase string) string {
	if v := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")); v != "" {
		return strings.TrimSuffix(v, "/")
	}
	return strings.TrimSuffix(defaultBase, "/")
}

// GroqBaseURL is the default Groq OpenAI-compatible root (Pipecat: GroqSTTService).
const GroqBaseURL = "https://api.groq.com/openai/v1"
