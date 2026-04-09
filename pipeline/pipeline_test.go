package pipeline

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestNewPipelineProcessorOrder(t *testing.T) {
	noop := func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		return nil
	}
	a := processor.Func{N: "a", F: noop}
	b := processor.Func{N: "b", F: noop}
	p := NewPipeline(a, b)
	ps := p.Processors()
	if len(ps) != 2 {
		t.Fatalf("len = %d", len(ps))
	}
	if ps[0].Name() != "a" || ps[1].Name() != "b" {
		t.Fatalf("names = %q, %q", ps[0].Name(), ps[1].Name())
	}
}
