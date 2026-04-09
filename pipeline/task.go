package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/processor"
)

// PipelineTask orchestrates frame flow through a Pipeline.
type PipelineTask struct {
	pipeline   *Pipeline
	observers  []observe.FrameObserver
	idleObs    *observe.IdleFrameObserver
	cancel     context.CancelFunc
	cancelOnce sync.Once
	mu         sync.Mutex
}

// NewPipelineTask builds a task for the given pipeline.
func NewPipelineTask(p *Pipeline, opts ...TaskOption) *PipelineTask {
	t := &PipelineTask{pipeline: p}
	for _, o := range opts {
		o(t)
	}
	return t
}

// TaskOption configures PipelineTask.
type TaskOption func(*PipelineTask)

// WithObservers registers frame observers (logging, metrics, idle, etc.).
func WithObservers(obs ...observe.FrameObserver) TaskOption {
	return func(t *PipelineTask) {
		t.observers = append(t.observers, obs...)
	}
}

// WithIdleObserver registers the idle watchdog (also added to observers).
func WithIdleObserver(idle *observe.IdleFrameObserver) TaskOption {
	return func(t *PipelineTask) {
		t.idleObs = idle
		t.observers = append(t.observers, idle)
	}
}

func (t *PipelineTask) procs() []processor.Processor {
	return t.pipeline.Processors()
}

func (t *PipelineTask) notifyObs(f frames.Frame, dir processor.Direction, idx int, name string) {
	p := observe.FramePushed{Frame: f, Direction: dir, Processor: name, Index: idx}
	for _, o := range t.observers {
		o.OnFrame(p)
	}
}

// processAt runs one frame starting at processor idx (recursive emit).
func (t *PipelineTask) processAt(ctx context.Context, idx int, f frames.Frame, dir processor.Direction) error {
	procs := t.procs()
	if idx < 0 || idx >= len(procs) {
		return nil
	}
	p := procs[idx]
	name := p.Name()
	t.notifyObs(f, dir, idx, name)

	emit := processor.Emit{
		Down: func(ff frames.Frame) {
			if idx+1 < len(procs) {
				_ = t.processAt(ctx, idx+1, ff, processor.Downstream)
			}
		},
		Up: func(ff frames.Frame) {
			if idx-1 >= 0 {
				_ = t.processAt(ctx, idx-1, ff, processor.Upstream)
			}
		},
	}
	return p.Process(ctx, f, dir, emit)
}

// QueueFrames injects frames at the start of the pipeline (downstream).
func (t *PipelineTask) QueueFrames(ctx context.Context, fs []frames.Frame) error {
	for _, f := range fs {
		if err := t.processAt(ctx, 0, f, processor.Downstream); err != nil {
			return err
		}
	}
	return nil
}

// Reenter continues downstream after processor at afterIdx (async STT/TTS callbacks).
func (t *PipelineTask) Reenter(ctx context.Context, afterIdx int, f frames.Frame) error {
	start := afterIdx + 1
	if start >= len(t.procs()) {
		return nil
	}
	return t.processAt(ctx, start, f, processor.Downstream)
}

// ReenterAfter resolves a processor name and continues downstream after it.
func (t *PipelineTask) ReenterAfter(ctx context.Context, processorName string, f frames.Frame) error {
	i := t.ProcessorIndex(processorName)
	if i < 0 {
		return fmt.Errorf("llmpipe: unknown processor %q", processorName)
	}
	return t.Reenter(ctx, i, f)
}

// ProcessorIndex returns the index of the first processor with the given name, or -1.
func (t *PipelineTask) ProcessorIndex(name string) int {
	for i, p := range t.procs() {
		if p.Name() == name {
			return i
		}
	}
	return -1
}

// Run executes the task until context is cancelled. Starts idle observer if set.
func (t *PipelineTask) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	if t.idleObs != nil {
		t.idleObs.Start()
		defer t.idleObs.Stop()
	}
	<-ctx.Done()
	return ctx.Err()
}

// Cancel stops the task context.
func (t *PipelineTask) Cancel() {
	t.cancelOnce.Do(func() {
		if t.cancel != nil {
			t.cancel()
		}
	})
}

// StartSession sends StartFrame through the pipeline.
func (t *PipelineTask) StartSession(ctx context.Context, sf *frames.StartFrame) error {
	return t.QueueFrames(ctx, []frames.Frame{sf})
}
