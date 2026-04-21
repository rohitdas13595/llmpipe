package turn

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// TrackingProcessor emits TurnStartedFrame / TurnEndedFrame around user speaking windows
// (Pipecat: TurnTrackingObserver turn boundaries).
type TrackingProcessor struct {
	name string
	// turnIndex counts started user turns (1-based in emitted frames).
	turnIndex int
	active    bool
}

// NewTrackingProcessor builds a processor that forwards all frames and injects turn metrics.
func NewTrackingProcessor(name string) *TrackingProcessor {
	return &TrackingProcessor{name: name}
}

func (t *TrackingProcessor) Name() string { return t.name }

func (t *TrackingProcessor) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch f.(type) {
	case *frames.UserStartedSpeakingFrame:
		t.turnIndex++
		t.active = true
		emit.Down(&frames.TurnStartedFrame{Index: t.turnIndex})
		emit.Down(f)
	case *frames.UserStoppedSpeakingFrame:
		idx := t.turnIndex
		if t.active {
			emit.Down(&frames.TurnEndedFrame{Index: idx, Complete: true})
		}
		t.active = false
		emit.Down(f)
	case *frames.InterruptionFrame:
		if t.active {
			emit.Down(&frames.TurnEndedFrame{Index: t.turnIndex, Complete: false})
			t.active = false
		}
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}
