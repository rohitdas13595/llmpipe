package elevenlabs

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// TTS calls ElevenLabs REST API with PCM output.
// It buffers streamed LLM text and synthesizes once per response so the API
// receives full sentences instead of one HTTP call per token (which often fails or is inaudible).
type TTS struct {
	name       string
	APIKey     string
	VoiceID    string
	ModelID    string
	SampleRate int
	Bot        *aggregate.BotState

	mu         sync.Mutex
	canceling  bool
	textBuffer strings.Builder
}

func NewTTS(name, apiKey, voiceID string, bot *aggregate.BotState, sampleRate int) *TTS {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &TTS{
		name:       name,
		APIKey:     apiKey,
		VoiceID:    voiceID,
		ModelID:    "eleven_turbo_v2_5",
		SampleRate: sampleRate,
		Bot:        bot,
	}
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
	if t.APIKey == "" {
		log.Println("elevenlabs: ELEVENLABS_API_KEY is empty")
		emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("elevenlabs: missing API key")})
		return
	}
	if strings.TrimSpace(t.VoiceID) == "" {
		log.Println("elevenlabs: ELEVENLABS_VOICE_ID is empty")
		emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("elevenlabs: missing voice id")})
		return
	}
	pcm, err := t.synth(ctx, text)
	if err != nil {
		log.Println("elevenlabs:", err)
		emit.Down(&frames.ErrorFrame{Err: err})
		return
	}
	if len(pcm) == 0 {
		log.Println("elevenlabs: empty PCM response")
		return
	}
	log.Printf("elevenlabs: synthesized %d bytes PCM (%d Hz) for %d chars", len(pcm), t.SampleRate, len(text))
	if t.Bot != nil {
		t.Bot.SetSpeaking(true)
	}
	emit.Down(&frames.BotSpeakingFrame{})
	emit.Down(&frames.TTSAudioRawFrame{
		Audio:       pcm,
		SampleRate:  t.SampleRate,
		NumChannels: 1,
	})
	emit.Down(&frames.BotStoppedSpeakingFrame{})
	if t.Bot != nil {
		t.Bot.SetSpeaking(false)
	}
}

func (t *TTS) synth(ctx context.Context, text string) ([]byte, error) {
	format := pcmOutputFormat(t.SampleRate)
	u := fmt.Sprintf(
		"https://api.elevenlabs.io/v1/text-to-speech/%s?output_format=%s",
		t.VoiceID,
		format,
	)
	payload := fmt.Sprintf(`{"text":%q,"model_id":%q}`, text, t.ModelID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("xi-api-key", t.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("elevenlabs: %s: %s", resp.Status, string(b))
	}
	return io.ReadAll(resp.Body)
}

func pcmOutputFormat(sampleRate int) string {
	switch sampleRate {
	case 8000:
		return "pcm_8000"
	case 22050:
		return "pcm_22050"
	case 24000:
		return "pcm_24000"
	case 44100:
		return "pcm_44100"
	case 48000:
		return "pcm_48000"
	default:
		return "pcm_16000"
	}
}
