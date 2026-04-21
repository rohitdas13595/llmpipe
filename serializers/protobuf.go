package serializers

import (
	"fmt"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/serializers/pipecatpb"
	"google.golang.org/protobuf/proto"
)

// Protobuf is a Pipecat-compatible binary serializer (pipecat/serializers/protobuf.py, frames.proto).
type Protobuf struct{}

// Serialize maps select llmpipe frames to pipecat.Frame protobuf bytes.
func (Protobuf) Serialize(f frames.Frame) ([]byte, error) {
	if f == nil {
		return nil, unsupportedType(f)
	}
	wrap := &pipecatpb.Frame{}
	switch fr := f.(type) {
	case *frames.TextFrame:
		wrap.Frame = &pipecatpb.Frame_Text{Text: &pipecatpb.TextFrame{Text: fr.Text}}
	case *frames.LLMTextFrame:
		wrap.Frame = &pipecatpb.Frame_Text{Text: &pipecatpb.TextFrame{Text: fr.Text}}
	case *frames.InputAudioRawFrame:
		wrap.Frame = &pipecatpb.Frame_Audio{Audio: &pipecatpb.AudioRawFrame{
			Audio:       append([]byte(nil), fr.Audio...),
			SampleRate:  uint32(fr.SampleRate),
			NumChannels: uint32(fr.NumChannels),
		}}
	case *frames.TTSAudioRawFrame:
		wrap.Frame = &pipecatpb.Frame_Audio{Audio: &pipecatpb.AudioRawFrame{
			Audio:       append([]byte(nil), fr.Audio...),
			SampleRate:  uint32(fr.SampleRate),
			NumChannels: uint32(fr.NumChannels),
		}}
	case *frames.TranscriptionFrame:
		wrap.Frame = &pipecatpb.Frame_Transcription{Transcription: &pipecatpb.TranscriptionFrame{Text: fr.Text}}
	case *frames.InterimTranscriptionFrame:
		wrap.Frame = &pipecatpb.Frame_Transcription{Transcription: &pipecatpb.TranscriptionFrame{Text: fr.Text}}
	case *frames.TransportMessageFrame:
		wrap.Frame = &pipecatpb.Frame_Message{Message: &pipecatpb.MessageFrame{Data: fr.Data}}
	default:
		return nil, unsupportedType(f)
	}
	return proto.Marshal(wrap)
}

// Deserialize parses protobuf bytes into llmpipe frames (Pipecat-compatible).
// Audio maps to InputAudioRawFrame (same as Pipecat inbound).
func (Protobuf) Deserialize(b []byte) (frames.Frame, error) {
	if len(b) == 0 {
		return nil, ErrDecode
	}
	var pf pipecatpb.Frame
	if err := proto.Unmarshal(b, &pf); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	switch pf.GetFrame().(type) {
	case *pipecatpb.Frame_Text:
		t := pf.GetText()
		if t == nil {
			return nil, ErrDecode
		}
		return &frames.TextFrame{Text: t.GetText()}, nil
	case *pipecatpb.Frame_Audio:
		a := pf.GetAudio()
		if a == nil {
			return nil, ErrDecode
		}
		return &frames.InputAudioRawFrame{
			Audio:       append([]byte(nil), a.GetAudio()...),
			SampleRate:  int(a.GetSampleRate()),
			NumChannels: int(a.GetNumChannels()),
		}, nil
	case *pipecatpb.Frame_Transcription:
		tr := pf.GetTranscription()
		if tr == nil {
			return nil, ErrDecode
		}
		return &frames.TranscriptionFrame{Text: tr.GetText()}, nil
	case *pipecatpb.Frame_Message:
		m := pf.GetMessage()
		if m == nil {
			return nil, ErrDecode
		}
		return &frames.TransportMessageFrame{Data: m.GetData()}, nil
	default:
		return nil, ErrDecode
	}
}
