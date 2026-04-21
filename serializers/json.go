package serializers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rohitdas13595/llmpipe/frames"
)

// JSON wraps llmpipe frames in a small tagged object for debugging and simple transports.
// This is not wire-compatible with Pipecat protobuf; use Protobuf for that.
type JSON struct{}

type jsonEnvelope struct {
	Kind        string          `json:"kind"`
	Text        string          `json:"text,omitempty"`
	AudioB64    string          `json:"audio_b64,omitempty"`
	SampleRate  int             `json:"sample_rate,omitempty"`
	NumChannels int             `json:"num_channels,omitempty"`
	Data        string          `json:"data,omitempty"`
	Index       int    `json:"index,omitempty"`
	Complete    *bool  `json:"complete,omitempty"`
}

// Serialize produces JSON with a required "kind" discriminator.
func (JSON) Serialize(f frames.Frame) ([]byte, error) {
	if f == nil {
		return nil, unsupportedType(f)
	}
	env := jsonEnvelope{}
	switch fr := f.(type) {
	case *frames.TextFrame:
		env.Kind = "text"
		env.Text = fr.Text
	case *frames.LLMTextFrame:
		env.Kind = "llm_text"
		env.Text = fr.Text
	case *frames.InputAudioRawFrame:
		env.Kind = "input_audio"
		env.AudioB64 = base64.StdEncoding.EncodeToString(fr.Audio)
		env.SampleRate = fr.SampleRate
		env.NumChannels = fr.NumChannels
	case *frames.TTSAudioRawFrame:
		env.Kind = "tts_audio"
		env.AudioB64 = base64.StdEncoding.EncodeToString(fr.Audio)
		env.SampleRate = fr.SampleRate
		env.NumChannels = fr.NumChannels
	case *frames.TranscriptionFrame:
		env.Kind = "transcription"
		env.Text = fr.Text
	case *frames.InterimTranscriptionFrame:
		env.Kind = "interim_transcription"
		env.Text = fr.Text
	case *frames.TransportMessageFrame:
		env.Kind = "transport_message"
		env.Data = fr.Data
	case *frames.TurnStartedFrame:
		env.Kind = "turn_started"
		env.Index = fr.Index
	case *frames.TurnEndedFrame:
		env.Kind = "turn_ended"
		env.Index = fr.Index
		c := fr.Complete
		env.Complete = &c
	default:
		return nil, unsupportedType(f)
	}
	return json.Marshal(env)
}

// Deserialize parses JSON from Serialize.
func (JSON) Deserialize(b []byte) (frames.Frame, error) {
	if len(strings.TrimSpace(string(b))) == 0 {
		return nil, ErrDecode
	}
	var env jsonEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	switch env.Kind {
	case "text":
		return &frames.TextFrame{Text: env.Text}, nil
	case "llm_text":
		return &frames.LLMTextFrame{Text: env.Text}, nil
	case "input_audio", "tts_audio":
		raw, err := base64.StdEncoding.DecodeString(env.AudioB64)
		if err != nil {
			return nil, fmt.Errorf("%w: audio_b64: %w", ErrDecode, err)
		}
		sr := env.SampleRate
		if sr <= 0 {
			sr = 16000
		}
		ch := env.NumChannels
		if ch <= 0 {
			ch = 1
		}
		if env.Kind == "input_audio" {
			return &frames.InputAudioRawFrame{Audio: raw, SampleRate: sr, NumChannels: ch}, nil
		}
		return &frames.TTSAudioRawFrame{Audio: raw, SampleRate: sr, NumChannels: ch}, nil
	case "transcription":
		return &frames.TranscriptionFrame{Text: env.Text}, nil
	case "interim_transcription":
		return &frames.InterimTranscriptionFrame{Text: env.Text}, nil
	case "transport_message":
		return &frames.TransportMessageFrame{Data: env.Data}, nil
	case "turn_started":
		return &frames.TurnStartedFrame{Index: env.Index}, nil
	case "turn_ended":
		c := true
		if env.Complete != nil {
			c = *env.Complete
		}
		return &frames.TurnEndedFrame{Index: env.Index, Complete: c}, nil
	default:
		return nil, fmt.Errorf("%w: unknown kind %q", ErrDecode, env.Kind)
	}
}
