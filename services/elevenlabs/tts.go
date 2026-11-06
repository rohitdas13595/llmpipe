package elevenlabs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/services/ttsbase"
)

// NewTTS is the ElevenLabs REST synthesizer wrapped by ttsbase.
// Sentences are spoken as they complete instead of waiting for the full reply.
// bot is accepted for compatibility; playback state comes from the output.
func NewTTS(name, apiKey, voiceID string, bot *aggregate.BotState, sampleRate int) *ttsbase.TTS {
	_ = bot
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return ttsbase.New(&restSynth{
		apiKey:     apiKey,
		voiceID:    voiceID,
		modelID:    "eleven_turbo_v2_5",
		sampleRate: sampleRate,
	}, ttsbase.Options{Name: name, SampleRate: sampleRate, PushTextFrames: true})
}

type restSynth struct {
	apiKey     string
	voiceID    string
	modelID    string
	sampleRate int
}

func (s *restSynth) Synthesize(ctx context.Context, text string, out func(ttsbase.AudioChunk) error) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if s.apiKey == "" {
		return fmt.Errorf("elevenlabs: missing API key")
	}
	if strings.TrimSpace(s.voiceID) == "" {
		return fmt.Errorf("elevenlabs: missing voice id")
	}
	pcm, err := s.synth(ctx, text)
	if err != nil {
		return err
	}
	if len(pcm) == 0 {
		return nil
	}
	return out(ttsbase.AudioChunk{PCM: pcm, SampleRate: s.sampleRate})
}

func (s *restSynth) synth(ctx context.Context, text string) ([]byte, error) {
	u := fmt.Sprintf(
		"https://api.elevenlabs.io/v1/text-to-speech/%s?output_format=%s",
		s.voiceID,
		pcmOutputFormat(s.sampleRate),
	)
	payload := fmt.Sprintf(`{"text":%q,"model_id":%q}`, text, s.modelID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("xi-api-key", s.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
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
