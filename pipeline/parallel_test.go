package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func identity(name string) processor.Func {
	return processor.Func{
		N: name,
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if dir == processor.Downstream {
				emit.Down(f)
			} else {
				emit.Up(f)
			}
			return nil
		},
	}
}

func TestParallelPipelineSingleBranch(t *testing.T) {
	var got []string
	p := NewPipeline(
		MustParallelPipeline("par", []processor.Processor{identity("a"), identity("b")}),
		processor.Func{
			N: "tail",
			F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
				if tf, ok := f.(*frames.TextFrame); ok {
					got = append(got, tf.Text)
				}
				return nil
			},
		},
	)
	task := NewPipelineTask(p)
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "hi"}}); err != nil {
			t.Fatal(err)
		}
	})
	if len(got) != 1 || got[0] != "hi" {
		t.Fatalf("got %v", got)
	}
}

func TestParallelPipelineDedupesSamePointer(t *testing.T) {
	var got []frames.Frame
	pp := MustParallelPipeline("par",
		[]processor.Processor{identity("l1")},
		[]processor.Processor{identity("l2")},
	)
	p := NewPipeline(pp, processor.Func{
		N: "sink",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			got = append(got, f)
			return nil
		},
	})
	task := NewPipelineTask(p)
	tf := &frames.TextFrame{Text: "once"}
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{tf}); err != nil {
			t.Fatal(err)
		}
	})
	if len(got) != 1 {
		t.Fatalf("expected 1 frame after dedup, got %d", len(got))
	}
	if got[0] != tf {
		t.Fatal("expected same pointer")
	}
}

func TestParallelPipelineStartFrameOrder(t *testing.T) {
	emitOnStart := processor.Func{
		N: "emitOnStart",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if dir == processor.Downstream {
				emit.Down(f)
			}
			if _, ok := f.(*frames.StartFrame); ok {
				emit.Down(&frames.TextFrame{Text: "from start"})
			}
			return nil
		},
	}
	pp := MustParallelPipeline("par",
		[]processor.Processor{emitOnStart},
		[]processor.Processor{identity("id")},
	)
	kinds := make(chan string, 8)
	p := NewPipeline(pp, processor.Func{
		N: "sink",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			kinds <- f.FrameKind()
			return nil
		},
	})
	task := NewPipelineTask(p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	// Data is queued first and observed before Start. Start is system priority,
	// so it would otherwise pass data still sitting in an inbox.
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case k := <-kinds:
		if k != frames.KindText {
			t.Fatalf("first frame kind %q", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for text")
	}
	if err := task.QueueFrame(context.Background(), &frames.StartFrame{SampleRate: 16000, NumChannels: 1}); err != nil {
		t.Fatal(err)
	}
	var rest []string
	timeout := time.After(2 * time.Second)
	for len(rest) < 2 {
		select {
		case k := <-kinds:
			rest = append(rest, k)
		case <-timeout:
			t.Fatalf("timeout, rest=%v", rest)
		}
	}
	task.StopWhenDone()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task timeout")
	}
	if rest[0] != frames.KindStart || rest[1] != frames.KindText {
		t.Fatalf("after text, got %v", rest)
	}
}

func TestParallelPipelineNewError(t *testing.T) {
	_, err := NewParallelPipeline("x")
	if err == nil {
		t.Fatal("expected error for no branches")
	}
}

func TestParallelPipelineUpstreamUsesBranch0(t *testing.T) {
	var saw string
	p0 := processor.Func{
		N: "p0",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if dir == processor.Upstream {
				if tf, ok := f.(*frames.TextFrame); ok {
					saw = tf.Text
				}
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
	pp := MustParallelPipeline("par", []processor.Processor{p0, p1})
	p := NewPipeline(pp)
	task := NewPipelineTask(p)
	runUntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "up"}}); err != nil {
			t.Fatal(err)
		}
	})
	if saw != "up" {
		t.Fatalf("upstream: %q", saw)
	}
}
