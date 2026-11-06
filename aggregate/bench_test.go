package aggregate

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func BenchmarkTurnEndToLLMRun(b *testing.B) {
	u := NewUserAggregator("ua", NewLLMContext(""), NewBotState(), interrupt.NoInterrupt{})
	u.SetParams(UserAggregatorParams{AggregationTimeout: 0})
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = u.Process(ctx, &frames.UserStartedSpeakingFrame{}, processor.Downstream, emit)
		_ = u.Process(ctx, &frames.TranscriptionFrame{Text: "hello"}, processor.Downstream, emit)
		_ = u.Process(ctx, &frames.UserStoppedSpeakingFrame{}, processor.Downstream, emit)
	}
}
