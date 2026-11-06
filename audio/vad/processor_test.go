package vad

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestProcessorEmitsVADFramesOnly(t *testing.T) {
	m := &scriptModel{sr: 16000, win: 160, conf: 1}
	a := NewStateAnalyzer(m, Params{Confidence: 0.5, StartSecs: 0, StopSecs: 0, MinVolume: 0})
	p := NewStateProcessor("vad", a)
	var kinds []string
	emit := processor.Emit{Down: func(f frames.Frame) { kinds = append(kinds, f.FrameKind()) }}
	loud := &frames.InputAudioRawFrame{Audio: pcmOf(160, 8000), SampleRate: 16000, NumChannels: 1}
	if err := p.Process(context.Background(), loud, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	m.conf = 0
	quiet := &frames.InputAudioRawFrame{Audio: pcmOf(160, 0), SampleRate: 16000, NumChannels: 1}
	if err := p.Process(context.Background(), quiet, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	want := []string{
		frames.KindVADUserStartedSpeaking,
		frames.KindInputAudio,
		frames.KindInputAudio,
		frames.KindVADUserStoppedSpeaking,
	}
	if len(kinds) != len(want) {
		t.Fatalf("kinds %v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("kinds %v", kinds)
		}
	}
}

func TestProcessorLegacyUserFrames(t *testing.T) {
	p := NewProcessor("vad", NewEnergyAnalyzer(100, 1, 1))
	p.LegacyEmitUserFrames = true
	var user int
	emit := processor.Emit{Down: func(f frames.Frame) {
		switch f.(type) {
		case *frames.UserStartedSpeakingFrame, *frames.UserStoppedSpeakingFrame:
			user++
		}
	}}
	loud := &frames.InputAudioRawFrame{Audio: pcmOf(160, 8000), SampleRate: 16000, NumChannels: 1}
	_ = p.Process(context.Background(), loud, processor.Downstream, emit)
	quiet := &frames.InputAudioRawFrame{Audio: pcmOf(160, 0), SampleRate: 16000, NumChannels: 1}
	_ = p.Process(context.Background(), quiet, processor.Downstream, emit)
	if user != 2 {
		t.Fatalf("user frames %d, want 2", user)
	}
}
