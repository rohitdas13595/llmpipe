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

func TestQueueFramesChainsDownstream(t *testing.T) {
	var order []string
	a := processor.Func{
		N: "a",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			order = append(order, "a")
			emit.Down(f)
			return nil
		},
	}
	b := processor.Func{
		N: "b",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			order = append(order, "b")
			emit.Down(f)
			return nil
		},
	}
	p := NewPipeline(a, b)
	task := NewPipelineTask(p)
	ctx := context.Background()
	if err := task.QueueFrames(ctx, []frames.Frame{&frames.TextFrame{Text: "x"}}); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("order = %v", order)
	}
}

func TestReenterAfterByName(t *testing.T) {
	var saw string
	end := processor.Func{
		N: "end",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				saw = tf.Text
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
	p := NewPipeline(mid, end)
	task := NewPipelineTask(p)
	ctx := context.Background()
	if err := task.ReenterAfter(ctx, "mid", &frames.TextFrame{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	if saw != "hi" {
		t.Fatalf("expected hi, got %q", saw)
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
			emit.Up(f)
			return nil
		},
	}
	p := NewPipeline(p0, p1)
	task := NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "x"}}); err != nil {
		t.Fatal(err)
	}
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
	p := NewPipeline(only)
	task := NewPipelineTask(p)
	if err := task.Reenter(context.Background(), 0, &frames.TextFrame{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("expected no processor after reenter past end, calls=%d", calls)
	}
}

func TestProcessorIndex(t *testing.T) {
	a := processor.Func{N: "dup", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error { return nil }}
	b := processor.Func{N: "dup", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error { return nil }}
	p := NewPipeline(a, b)
	task := NewPipelineTask(p)
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
			return want
		},
	}
	p := NewPipeline(bad)
	task := NewPipelineTask(p)
	err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{}})
	if err != want {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestWithObserversNotified(t *testing.T) {
	var last observe.FramePushed
	o := observe.FuncObserver{F: func(p observe.FramePushed) { last = p }}
	rec := processor.Func{
		N: "rec",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			return nil
		},
	}
	p := NewPipeline(rec)
	task := NewPipelineTask(p, WithObservers(o))
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "z"}}); err != nil {
		t.Fatal(err)
	}
	if last.Processor != "rec" || last.Index != 0 {
		t.Fatalf("observer: %+v", last)
	}
	if tf, ok := last.Frame.(*frames.TextFrame); !ok || tf.Text != "z" {
		t.Fatalf("frame: %+v", last.Frame)
	}
}

func TestStartSession(t *testing.T) {
	var saw int
	p := NewPipeline(processor.Func{
		N: "x",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.StartFrame); ok {
				saw++
			}
			return nil
		},
	})
	task := NewPipelineTask(p)
	sf := &frames.StartFrame{SampleRate: 16000, NumChannels: 1}
	if err := task.StartSession(context.Background(), sf); err != nil {
		t.Fatal(err)
	}
	if saw != 1 {
		t.Fatalf("StartSession: saw=%d", saw)
	}
}

func TestRunUntilCancel(t *testing.T) {
	p := NewPipeline(processor.Func{N: "noop", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
		emit.Down(f)
		return nil
	}})
	task := NewPipelineTask(p)
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
	p := NewPipeline(processor.Func{N: "noop", F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error { return nil }})
	task := NewPipelineTask(p)
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
	p := NewPipeline(a)
	task := NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TextFrame{Text: "1"},
		&frames.TextFrame{Text: "2"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("got %v", got)
	}
}
