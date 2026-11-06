package ttsbase

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func BenchmarkFirstAudioAfterLLMToken(b *testing.B) {
	s := synthFunc(func(ctx context.Context, text string, out func(AudioChunk) error) error {
		return out(AudioChunk{PCM: []byte{0, 0, 1, 0}, SampleRate: 16000})
	})
	tts := New(s, Options{Name: "tts", SampleRate: 16000, PushTextFrames: true})
	var audio int
	emit := processor.Emit{Down: func(f frames.Frame) {
		if _, ok := f.(*frames.TTSAudioRawFrame); ok {
			audio++
		}
	}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tts.Process(ctx, &frames.LLMFullResponseStartFrame{}, processor.Downstream, emit)
		_ = tts.Process(ctx, &frames.LLMTextFrame{Text: "Hello there. Next"}, processor.Downstream, emit)
		_ = tts.Process(ctx, &frames.LLMFullResponseEndFrame{}, processor.Downstream, emit)
	}
	b.StopTimer()
	if audio == 0 {
		b.Fatal("no audio")
	}
}
