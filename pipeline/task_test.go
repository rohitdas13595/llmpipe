package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/processor"
)

func waitRunning(t *testing.T, task *PipelineTask, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		select {
		case err := <-done:
			t.Fatalf("task ended early: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
}

// runUntilEnd starts the task, runs send once the loops are up, then End.
// Sends that queue at the head stay ordered with that End.
func runUntilEnd(t *testing.T, task *PipelineTask, send func()) {
	t.Helper()
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		task.Cancel()
	})
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
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

func TestQueueFramesChainsDownstream(t *testing.T) {
	var order []string
	a := processor.Func{
		N: "a",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				order = append(order, "a")
			}
			emit.Down(f)
			return nil
		},
	}
	b := processor.Func{
		N: "b",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				order = append(order, "b")
			}
			emit.Down(f)
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(a, b))
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "x"}}); err != nil {
			t.Fatal(err)
		}
	})
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v", order)
	}
}

func TestReenterAfterByName(t *testing.T) {
	saw := make(chan string, 1)
	end := processor.Func{
		N: "end",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				saw <- tf.Text
			}
			return nil
		},
	}
	mid := processor.Func{
		N: "mid",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(mid, end))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.ReenterAfter(ctx, "mid", &frames.TextFrame{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-saw:
		if got != "hi" {
			t.Fatalf("expected hi, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reenter timeout")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestEmitUpstream(t *testing.T) {
	var sawUpstream bool
	p0 := processor.Func{
		N: "p0",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if dir == processor.Upstream {
				sawUpstream = true
				return nil
			}
			emit.Down(f)
			return nil
		},
	}
	p1 := processor.Func{
		N: "p1",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				emit.Up(f)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p0, p1))
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "x"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !sawUpstream {
		t.Fatal("expected upstream delivery at p0")
	}
}

func TestReenterAfterUnknownProcessor(t *testing.T) {
	p := NewPipeline(processor.Func{N: "only", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		emit.Down(f)
		return nil
	}})
	task := NewPipelineTask(p)
	err := task.ReenterAfter(context.Background(), "nope", &frames.TextFrame{Text: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReenterPastEndNoOp(t *testing.T) {
	var calls int
	only := processor.Func{
		N: "only",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			calls++
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(only))
	if err := task.Reenter(context.Background(), 0, &frames.TextFrame{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("expected no processor after reenter past end, calls=%d", calls)
	}
}

func TestProcessorIndex(t *testing.T) {
	a := processor.Func{N: "dup", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		return nil
	}}
	b := processor.Func{N: "dup", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		return nil
	}}
	task := NewPipelineTask(NewPipeline(a, b))
	if task.ProcessorIndex("dup") != 0 {
		t.Fatalf("first dup index = %d", task.ProcessorIndex("dup"))
	}
	if task.ProcessorIndex("missing") != -1 {
		t.Fatal("expected -1 for missing")
	}
}

func TestQueueFramesPropagatesProcessError(t *testing.T) {
	want := errors.New("boom")
	bad := processor.Func{
		N: "bad",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			return want
		},
	}
	errCh := make(chan error, 1)
	task := NewPipelineTask(NewPipeline(bad), WithOnError(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != want {
			t.Fatalf("err = %v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("error was not delivered upstream")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestWithObserversNotified(t *testing.T) {
	var last observe.FramePushed
	o := observe.FuncObserver{F: func(p observe.FramePushed) {
		if _, ok := p.Frame.(*frames.TextFrame); ok {
			last = p
		}
	}}
	rec := processor.Func{
		N: "rec",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(rec), WithObservers(o))
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "z"}}); err != nil {
			t.Fatal(err)
		}
	})
	if last.Processor != "rec" || last.Index != 0 {
		t.Fatalf("observer: %+v", last)
	}
	if tf, ok := last.Frame.(*frames.TextFrame); !ok || tf.Text != "z" {
		t.Fatalf("frame: %+v", last.Frame)
	}
}

func TestStartSession(t *testing.T) {
	var saw int
	task := NewPipelineTask(NewPipeline(processor.Func{
		N: "x",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.StartFrame); ok {
				saw++
			}
			return nil
		},
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	sf := &frames.StartFrame{SampleRate: 16000, NumChannels: 1}
	if err := task.StartSession(context.Background(), sf); err != nil {
		t.Fatal(err)
	}
	if saw != 1 {
		t.Fatalf("StartSession: saw=%d", saw)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRunUntilCancel(t *testing.T) {
	task := NewPipelineTask(NewPipeline(processor.Func{N: "noop", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		emit.Down(f)
		return nil
	}}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestCancelIdempotent(t *testing.T) {
	task := NewPipelineTask(NewPipeline(processor.Func{N: "noop", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		return nil
	}}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = task.Run(ctx)
	task.Cancel()
	task.Cancel()
}

func TestQueueFramesMultipleFramesOrder(t *testing.T) {
	var got []string
	a := processor.Func{
		N: "a",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				got = append(got, tf.Text)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(a))
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.TextFrame{Text: "1"},
			&frames.TextFrame{Text: "2"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("got %v", got)
	}
}
