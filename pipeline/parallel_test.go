package pipeline

import (
	"context"
	"testing"

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
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "hi"}}); err != nil {
		t.Fatal(err)
	}
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
			got = append(got, f)
			return nil
		},
	})
	task := NewPipelineTask(p)
	tf := &frames.TextFrame{Text: "once"}
	if err := task.QueueFrames(context.Background(), []frames.Frame{tf}); err != nil {
		t.Fatal(err)
	}
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
	var kinds []string
	p := NewPipeline(pp, processor.Func{
		N: "sink",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			kinds = append(kinds, f.FrameKind())
			return nil
		},
	})
	task := NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TextFrame{Text: "hello"},
		&frames.StartFrame{SampleRate: 16000, NumChannels: 1},
	}); err != nil {
		t.Fatal(err)
	}
	// First queued frame: TextFrame (data) — parallel emits one TextFrame
	// Second: StartFrame sync — Start, then Text from start
	if len(kinds) < 2 {
		t.Fatalf("kinds: %v", kinds)
	}
	if kinds[0] != frames.KindText {
		t.Fatalf("first frame kind %q", kinds[0])
	}
	// Find Start sequence
	var startIdx int = -1
	for i, k := range kinds {
		if k == frames.KindStart {
			startIdx = i
			break
		}
	}
	if startIdx < 0 {
		t.Fatalf("no StartFrame in %v", kinds)
	}
	if startIdx+1 >= len(kinds) || kinds[startIdx+1] != frames.KindText {
		t.Fatalf("expected Text after Start, got %v", kinds[startIdx:])
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
	if err := task.QueueFrames(context.Background(), []frames.Frame{&frames.TextFrame{Text: "up"}}); err != nil {
		t.Fatal(err)
	}
	if saw != "up" {
		t.Fatalf("upstream: %q", saw)
	}
}
