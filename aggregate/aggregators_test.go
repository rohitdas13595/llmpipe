package aggregate

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestUserAggregatorTranscriptionEmitsLLMRun(t *testing.T) {
	ctxBag := NewLLMContext("")
	bot := NewBotState()
	u := NewUserAggregator("ua", ctxBag, bot, interrupt.NoInterrupt{})
	var kinds []string
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			kinds = append(kinds, f.FrameKind())
			return nil
		},
	}
	p := pipeline.NewPipeline(u, tail)
	task := pipeline.NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TranscriptionFrame{Text: "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 1 || kinds[0] != frames.KindLLMRun {
		t.Fatalf("kinds = %v", kinds)
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["content"] != "hello" {
		t.Fatalf("context = %#v", msgs)
	}
}

func TestUserAggregatorEmptyTranscriptionNoLLMRun(t *testing.T) {
	ctxBag := NewLLMContext("")
	u := NewUserAggregator("ua", ctxBag, NewBotState(), interrupt.NoInterrupt{})
	var kinds []string
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			kinds = append(kinds, f.FrameKind())
			return nil
		},
	}
	p := pipeline.NewPipeline(u, tail)
	task := pipeline.NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TranscriptionFrame{Text: "  "},
	}); err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 1 || kinds[0] != frames.KindTranscription {
		t.Fatalf("kinds = %v", kinds)
	}
}

func TestAssistantAggregatorStreamsTextAndCommits(t *testing.T) {
	ctxBag := NewLLMContext("")
	a := NewAssistantAggregator("asst", ctxBag)
	var texts []string
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				texts = append(texts, tf.Text)
			}
			return nil
		},
	}
	p := pipeline.NewPipeline(a, tail)
	task := pipeline.NewPipelineTask(p)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "he"},
		&frames.LLMTextFrame{Text: "llo"},
		&frames.LLMFullResponseEndFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 2 || texts[0]+texts[1] != "hello" {
		t.Fatalf("texts = %v", texts)
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["role"] != "assistant" || msgs[0]["content"] != "hello" {
		t.Fatalf("assistant context = %#v", msgs)
	}
}

func TestBotStateSpeaking(t *testing.T) {
	b := NewBotState()
	if b.Speaking() {
		t.Fatal("expected false")
	}
	b.SetSpeaking(true)
	if !b.Speaking() {
		t.Fatal("expected true")
	}
	b.SetSpeaking(false)
	if b.Speaking() {
		t.Fatal("expected false")
	}
}
