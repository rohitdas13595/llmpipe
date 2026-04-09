// Package observe defines pipeline frame observers.
package observe

import (
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// FrameObserver receives notifications when frames flow through the task.
type FrameObserver interface {
	OnFrame(pushed FramePushed)
}

// FramePushed is observer event data.
type FramePushed struct {
	Frame      frames.Frame
	Direction  processor.Direction
	Processor  string
	Index      int
}

// FuncObserver adapts a function to FrameObserver.
type FuncObserver struct {
	F func(FramePushed)
}

func (f FuncObserver) OnFrame(p FramePushed) {
	if f.F != nil {
		f.F(p)
	}
}
