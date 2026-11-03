package deepgram

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestEchoGateOffKeepsAudioWhileBotSpeaks(t *testing.T) {
	bot := aggregate.NewBotState()
	bot.SetSpeaking(true)
	s := NewSTT("stt", "", nil, 16000, bot)
	emit := processor.Emit{Down: func(frames.Frame) {}}
	pcm := []byte{1, 2, 3, 4}
	if err := s.Process(context.Background(), &frames.InputAudioRawFrame{Audio: pcm, SampleRate: 16000}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if s.buf.Len() != len(pcm) {
		t.Fatalf("buf %d, want audio kept while bot speaks", s.buf.Len())
	}
}

func TestMuteWhileBotSpeakingDropsAudio(t *testing.T) {
	bot := aggregate.NewBotState()
	bot.SetSpeaking(true)
	s := NewSTT("stt", "", nil, 16000, bot)
	s.MuteWhileBotSpeaking = true
	emit := processor.Emit{Down: func(frames.Frame) {}}
	if err := s.Process(context.Background(), &frames.InputAudioRawFrame{Audio: []byte{1, 2, 3, 4}, SampleRate: 16000}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if s.buf.Len() != 0 {
		t.Fatalf("buf %d, want muted", s.buf.Len())
	}
}
