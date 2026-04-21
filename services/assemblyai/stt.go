// Package assemblyai provides batch STT via AssemblyAI REST v2 (Pipecat: AssemblyAISTTService uses streaming WS; llmpipe uses upload+poll per utterance).
package assemblyai

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/internal/audiofmt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// STT uploads WAV after each utterance and polls for the transcript.
type STT struct {
	name       string
	APIKey     string
	SampleRate int
	Reenter    services.ReenterFunc
	Bot        *aggregate.BotState

	mu  sync.Mutex
	buf bytes.Buffer
}

func NewSTT(name, apiKey string, reenter services.ReenterFunc, sampleRate int, bot *aggregate.BotState) *STT {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &STT{name: name, APIKey: apiKey, SampleRate: sampleRate, Reenter: reenter, Bot: bot}
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

type uploadResp struct {
	UploadURL string `json:"upload_url"`
}

type transcriptCreate struct {
	AudioURL string `json:"audio_url"`
}

type transcriptStatus struct {
	ID     string `json:"id"`
	Status string `json:"status"` // queued, processing, completed, error
	Text   string `json:"text"`
	Error  string `json:"error"`
}

func (s *STT) flush(ctx context.Context) {
	s.mu.Lock()
	pcm := append([]byte(nil), s.buf.Bytes()...)
	s.buf.Reset()
	s.mu.Unlock()
	if s.Reenter == nil {
		return
	}
	if strings.TrimSpace(s.APIKey) == "" {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: fmt.Errorf("assemblyai: set ASSEMBLYAI_API_KEY")})
		return
	}
	if !gate(pcm, s.SampleRate) {
		return
	}
	wav := audiofmt.MonoS16LE(pcm, s.SampleRate)

	uploadURL, err := s.upload(ctx, wav)
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	id, err := s.createTranscript(ctx, uploadURL)
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	text, err := s.poll(ctx, id)
	if err != nil {
		_ = s.Reenter(ctx, s.name, &frames.ErrorFrame{Err: err})
		return
	}
	if strings.TrimSpace(text) == "" {
		return
	}
	services.PipelineLog("stt", "assemblyai transcript: %q", text)
	_ = s.Reenter(ctx, s.name, &frames.TranscriptionFrame{Text: strings.TrimSpace(text)})
}

func (s *STT) upload(ctx context.Context, wav []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.assemblyai.com/v2/upload", bytes.NewReader(wav))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", s.APIKey)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("assemblyai upload: %s: %s", resp.Status, string(body))
	}
	var out uploadResp
	if json.Unmarshal(body, &out) != nil || out.UploadURL == "" {
		return "", fmt.Errorf("assemblyai upload: invalid json")
	}
	return out.UploadURL, nil
}

func (s *STT) createTranscript(ctx context.Context, audioURL string) (string, error) {
	payload, _ := json.Marshal(transcriptCreate{AudioURL: audioURL})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.assemblyai.com/v2/transcript", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", s.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("assemblyai transcript create: %s: %s", resp.Status, string(body))
	}
	var ts transcriptStatus
	if json.Unmarshal(body, &ts) != nil || ts.ID == "" {
		return "", fmt.Errorf("assemblyai: invalid create response")
	}
	return ts.ID, nil
}

func (s *STT) poll(ctx context.Context, id string) (string, error) {
	deadline := time.Now().Add(120 * time.Second)
	u := "https://api.assemblyai.com/v2/transcript/" + id
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", s.APIKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var ts transcriptStatus
		if json.Unmarshal(body, &ts) != nil {
			return "", fmt.Errorf("assemblyai: invalid poll json")
		}
		switch strings.ToLower(ts.Status) {
		case "completed":
			return ts.Text, nil
		case "error":
			return "", fmt.Errorf("assemblyai: %s", ts.Error)
		default:
			time.Sleep(350 * time.Millisecond)
		}
	}
	return "", fmt.Errorf("assemblyai: poll timeout")
}

func gate(pcm []byte, sampleRate int) bool {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	minB := sampleRate * 2 / 10
	if minB < 3200 {
		minB = 3200
	}
	if len(pcm) < minB {
		return false
	}
	var sum float64
	for i := 0; i+1 < len(pcm); i += 2 {
		v := int16(binary.LittleEndian.Uint16(pcm[i : i+2]))
		sum += float64(v) * float64(v)
	}
	rms := math.Sqrt(sum / float64(len(pcm)/2))
	minR := 55.0
	if v := os.Getenv("STT_MIN_RMS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			minR = f
		}
	}
	return rms >= minR
}
