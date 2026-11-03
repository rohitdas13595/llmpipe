package providers

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestUserTurnsMuteDuringFunctionCalls(t *testing.T) {
	p := BuildUserTurns()
	var audio int
	emit := processor.Emit{Down: func(f frames.Frame) {
		if _, ok := f.(*frames.InputAudioRawFrame); ok {
			audio++
		}
	}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	started := &frames.FunctionCallsStartedFrame{Batch: []tools.Call{{ID: "a"}, {ID: "b"}}}
	if err := p.Process(ctx, started, processor.Upstream, emit); err != nil {
		t.Fatal(err)
	}
	if err := p.Process(ctx, &frames.InputAudioRawFrame{Audio: []byte{1, 0}}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if audio != 0 {
		t.Fatal("audio forwarded while calls are in flight")
	}
	_ = p.Process(ctx, &frames.FunctionCallResultFrame{CallID: "a"}, processor.Upstream, emit)
	_ = p.Process(ctx, &frames.FunctionCallCancelFrame{CallID: "b"}, processor.Upstream, emit)
	if err := p.Process(ctx, &frames.InputAudioRawFrame{Audio: []byte{1, 0}}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if audio != 1 {
		t.Fatalf("audio frames after the calls = %d", audio)
	}
}
