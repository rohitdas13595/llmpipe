package deepgram

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// STT uses Deepgram prerecorded API when the user stops speaking (VAD).
type STT struct {
	name       string
	APIKey     string
	Model      string
	SampleRate int
	Reenter    services.ReenterFunc
	// Bot, when set, is used to ignore mic audio while TTS is playing (reduces echo → empty transcripts).
	Bot *aggregate.BotState

	mu  sync.Mutex
	buf bytes.Buffer
}

func NewSTT(name, apiKey string, reenter services.ReenterFunc, sampleRate int, bot *aggregate.BotState) *STT {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &STT{
		name:       name,
		APIKey:     apiKey,
		Model:      "nova-2",
		SampleRate: sampleRate,
		Reenter:    reenter,
		Bot:        bot,
	}
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

type dgResp struct {
	Results struct {
		Channels []struct {
			Alternatives []struct {
				Transcript string `json:"transcript"`
			} `json:"alternatives"`
		} `json:"channels"`
	} `json:"results"`
}

func (s *STT) flush(ctx context.Context) {
	s.mu.Lock()
	audio := append([]byte(nil), s.buf.Bytes()...)
	s.buf.Reset()
	s.mu.Unlock()
	if s.Reenter == nil {
		return
	}
	if s.APIKey == "" {
		services.PipelineLog("stt", "deepgram: DEEPGRAM_API_KEY is empty; set it in .env")
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: fmt.Errorf("deepgram: missing API key")})
		return
	}
	if len(audio) == 0 {
		services.PipelineLog("stt", "deepgram: flush skipped (no audio in buffer — did VAD fire end-of-utterance?)")
		return
	}
	if !shouldSendPCMToDeepgram(audio, s.SampleRate) {
		return
	}
	services.PipelineLog("stt", "deepgram: sending %d bytes PCM @ %d Hz for transcript", len(audio), s.SampleRate)
	q := url.Values{}
	q.Set("model", s.Model)
	q.Set("encoding", "linear16")
	q.Set("channels", "1")
	q.Set("sample_rate", fmt.Sprintf("%d", s.SampleRate))
	q.Set("smart_format", "true")
	u := "https://api.deepgram.com/v1/listen?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(audio))
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("Authorization", "Token "+s.APIKey)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		services.PipelineLog("stt", "deepgram: %v", err)
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("deepgram: %s: %s", resp.Status, string(body))
		services.PipelineLog("stt", "deepgram: %v", err)
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	var out dgResp
	if json.Unmarshal(body, &out) != nil {
		services.PipelineLog("stt", "deepgram: invalid json (first 200 chars): %.200q", string(body))
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: fmt.Errorf("deepgram: invalid json")})
		return
	}
	text := ""
	if len(out.Results.Channels) > 0 && len(out.Results.Channels[0].Alternatives) > 0 {
		text = out.Results.Channels[0].Alternatives[0].Transcript
	}
	if text == "" {
		if os.Getenv("STT_EMPTY_LOG") == "1" {
			services.PipelineLog("stt", "deepgram: no words in %d bytes @ %d Hz (set STT_EMPTY_LOG=0 to hide)", len(audio), s.SampleRate)
		}
		return
	}
	services.PipelineLog("stt", "deepgram transcript: %q", text)
	_ = s.Reenter(ctx, s.name, &frames.TranscriptionFrame{Text: text})
}

// pcmRMS16LE is RMS amplitude for s16le mono (same scale as energy VAD, 0..~32768).
func pcmRMS16LE(pcm []byte) float64 {
	if len(pcm) < 2 {
		return 0
	}
	var sum float64
	for i := 0; i+1 < len(pcm); i += 2 {
		s := int16(binary.LittleEndian.Uint16(pcm[i : i+2]))
		sum += float64(s) * float64(s)
	}
	n := float64(len(pcm) / 2)
	return math.Sqrt(sum / n)
}

// shouldSendPCMToDeepgram avoids HTTP when audio is too short or too quiet (no “words” proxy).
// Env: STT_MIN_PCM_BYTES (default ~100ms at sampleRate), STT_MIN_RMS (default 55), STT_GATE_LOG=1 to log skips.
func shouldSendPCMToDeepgram(pcm []byte, sampleRate int) bool {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	minBytes := sampleRate * 2 / 10 // ~100 ms mono s16le
	if minBytes < 3200 {
		minBytes = 3200
	}
	if v := os.Getenv("STT_MIN_PCM_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			minBytes = n
		}
	}
	if len(pcm) < minBytes {
		if os.Getenv("STT_GATE_LOG") == "1" {
			services.PipelineLog("stt", "deepgram: skip API (buffer %d bytes < min %d)", len(pcm), minBytes)
		}
		return false
	}
	minRMS := 55.0
	if v := os.Getenv("STT_MIN_RMS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			minRMS = f
		}
	}
	r := pcmRMS16LE(pcm)
	if r < minRMS {
		if os.Getenv("STT_GATE_LOG") == "1" {
			services.PipelineLog("stt", "deepgram: skip API (RMS %.1f < %.1f, %d bytes)", r, minRMS, len(pcm))
		}
		return false
	}
	return true
}
