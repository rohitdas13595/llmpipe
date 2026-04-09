// Package processor defines the FrameProcessor abstraction and direction.
package processor

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
)

// Direction is frame flow direction (downstream or upstream).
type Direction int

const (
	Downstream Direction = 1
	Upstream   Direction = 2
)

// Emit forwards frames to adjacent processors.
type Emit struct {
	Down func(frames.Frame)
	Up   func(frames.Frame)
}

// Processor handles frames in the pipeline.
type Processor interface {
	Name() string
	// Process handles one frame; use emit to forward (zero or more times).
	Process(ctx context.Context, f frames.Frame, dir Direction, emit Emit) error
}

// Func is an adapter for a single function processor.
type Func struct {
	N string
	F func(ctx context.Context, f frames.Frame, dir Direction, emit Emit) error
}

func (fn Func) Name() string { return fn.N }

func (fn Func) Process(ctx context.Context, f frames.Frame, dir Direction, emit Emit) error {
	return fn.F(ctx, f, dir, emit)
}
