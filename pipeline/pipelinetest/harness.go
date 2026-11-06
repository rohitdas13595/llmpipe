// Package pipelinetest is a small harness for queued pipeline tests.
package pipelinetest

import (
	"context"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
)

// UntilEnd starts task, runs send after the node loops accept frames, queues
// EndFrame, and waits until Run returns nil.
func UntilEnd(t *testing.T, task *pipeline.PipelineTask, send func()) {
	t.Helper()
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		task.Cancel()
	})
	go func() { done <- task.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("pipeline task did not start")
		}
		select {
		case err := <-done:
			t.Fatalf("task ended early: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	if send != nil {
		send()
	}
	task.StopWhenDone()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("task: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task timeout")
	}
}

// Recorder is a sink processor that keeps the frames it is shown.
type Recorder struct {
	mu     chan struct{}
	frames []frames.Frame
}

// NewRecorder builds an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{mu: make(chan struct{}, 1)}
}

func (r *Recorder) lock() {
	r.mu <- struct{}{}
}

func (r *Recorder) unlock() { <-r.mu }

// Processor returns a processor named name that appends every frame except
// End and Cancel.
func (r *Recorder) Processor(name string) processor.Processor {
	return processor.Func{
		N: name,
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			switch f.(type) {
			case *frames.EndFrame, *frames.CancelFrame:
			default:
				r.lock()
				r.frames = append(r.frames, f)
				r.unlock()
			}
			return nil
		},
	}
}

// Frames returns a copy of the recorded frames.
func (r *Recorder) Frames() []frames.Frame {
	r.lock()
	defer r.unlock()
	out := make([]frames.Frame, len(r.frames))
	copy(out, r.frames)
	return out
}
