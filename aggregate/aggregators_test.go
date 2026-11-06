package aggregate

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestUserAggregatorTranscriptionEmitsLLMRun(t *testing.T) {
	ctxBag := NewLLMContext("")
	bot := NewBotState()
	u := NewUserAggregator("ua", ctxBag, bot, interrupt.NoInterrupt{})
	u.SetParams(UserAggregatorParams{AggregationTimeout: 0})
	var kinds []string
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			kinds = append(kinds, f.FrameKind())
			return nil
		},
	}
	p := pipeline.NewPipeline(u, tail)
	task := pipeline.NewPipelineTask(p)
	pipelinetest.UntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.TranscriptionFrame{Text: "hello"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if len(kinds) != 1 || kinds[0] != frames.KindLLMRun {
		t.Fatalf("kinds = %v", kinds)
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["content"] != "hello" {
		t.Fatalf("context = %#v", msgs)
	}
}

func TestOneLLMRunPerTurn(t *testing.T) {
	// Process directly. Queuing these together on a task would let the system
	// lane run UserStopped ahead of the data-lane transcripts.
	ctxBag := NewLLMContext("")
	u := NewUserAggregator("ua", ctxBag, NewBotState(), interrupt.NoInterrupt{})
	u.SetParams(UserAggregatorParams{AggregationTimeout: 0})
	var runs int
	emit := processor.Emit{Down: func(f frames.Frame) {
		if _, ok := f.(*frames.LLMRunFrame); ok {
			runs++
		}
	}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	steps := []frames.Frame{
		&frames.UserStartedSpeakingFrame{},
		&frames.TranscriptionFrame{Text: "hello"},
		&frames.TranscriptionFrame{Text: "there"},
		&frames.UserStoppedSpeakingFrame{},
	}
	for _, f := range steps {
		if err := u.Process(ctx, f, processor.Downstream, emit); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 {
		t.Fatalf("llm runs = %d, want 1", runs)
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["content"] != "hello there" {
		t.Fatalf("context = %#v", msgs)
	}
}

func TestAggregationTimeoutCoalescesLateFinal(t *testing.T) {
	ctxBag := NewLLMContext("")
	u := NewUserAggregator("ua", ctxBag, NewBotState(), interrupt.NoInterrupt{})
	u.SetParams(UserAggregatorParams{AggregationTimeout: 120 * time.Millisecond})
	var runs atomic.Int32
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.LLMRunFrame); ok {
				runs.Add(1)
			}
			return nil
		},
	}
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(u, tail))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.UserStartedSpeakingFrame{},
		&frames.TranscriptionFrame{Text: "hello"},
		&frames.UserStoppedSpeakingFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if runs.Load() != 0 {
		t.Fatalf("ran before timeout: %d", runs.Load())
	}
	if err := task.QueueFrame(context.Background(), &frames.TranscriptionFrame{Text: "there"}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for runs.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("runs = %d", runs.Load())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond)
	if runs.Load() != 1 {
		t.Fatalf("extra runs = %d", runs.Load())
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["content"] != "hello there" {
		t.Fatalf("context = %#v", msgs)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
	}
}

func TestUserAggregatorEmptyTranscriptionNoLLMRun(t *testing.T) {
	ctxBag := NewLLMContext("")
	u := NewUserAggregator("ua", ctxBag, NewBotState(), interrupt.NoInterrupt{})
	var kinds []string
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			kinds = append(kinds, f.FrameKind())
			return nil
		},
	}
	p := pipeline.NewPipeline(u, tail)
	task := pipeline.NewPipelineTask(p)
	pipelinetest.UntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.TranscriptionFrame{Text: "  "},
		}); err != nil {
			t.Fatal(err)
		}
	})
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
	pipelinetest.UntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.LLMFullResponseStartFrame{},
			&frames.LLMTextFrame{Text: "he"},
			&frames.LLMTextFrame{Text: "llo"},
			&frames.LLMFullResponseEndFrame{},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if len(texts) != 2 || texts[0]+texts[1] != "hello" {
		t.Fatalf("texts = %v", texts)
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["role"] != "assistant" || msgs[0]["content"] != "hello" {
		t.Fatalf("assistant context = %#v", msgs)
	}
}

func TestUserAggregatorDoesNotInterrupt(t *testing.T) {
	// Barge-in moved to turns.UserTurnProcessor. The aggregator must not
	// ask for an interruption on its own.
	ctxBag := NewLLMContext("")
	bot := NewBotState()
	bot.SetSpeaking(true)
	u := NewUserAggregator("ua", ctxBag, bot, interrupt.MinWords{N: 1})
	var interrupts int
	emit := processor.Emit{
		Down: func(f frames.Frame) {
			if _, ok := f.(*frames.InterruptionFrame); ok {
				interrupts++
			}
		},
		Up: func(f frames.Frame) {
			if _, ok := f.(*frames.InterruptionTaskFrame); ok {
				interrupts++
			}
		},
	}
	if err := u.Process(context.Background(), &frames.InterimTranscriptionFrame{Text: "hey"}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	audio := bytes.Repeat([]byte{1, 0}, 16000)
	_ = u.Process(context.Background(), &frames.UserStartedSpeakingFrame{}, processor.Downstream, emit)
	_ = u.Process(context.Background(), &frames.InputAudioRawFrame{Audio: audio, SampleRate: 16000}, processor.Downstream, emit)
	if interrupts != 0 {
		t.Fatalf("aggregator interruptions = %d, want 0", interrupts)
	}
}

func TestAssistantInterruptCommitsPlayedSentence(t *testing.T) {
	ctxBag := NewLLMContext("")
	a := NewAssistantAggregator("asst", ctxBag)
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	steps := []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "Hello there. And more words."},
		&frames.TTSStartedFrame{},
		&frames.TTSTextFrame{Text: "Hello there."},
		&frames.InterruptionFrame{},
		&frames.TTSTextFrame{Text: "And more words."},
	}
	for _, f := range steps {
		if err := a.Process(ctx, f, processor.Downstream, emit); err != nil {
			t.Fatal(err)
		}
	}
	msgs := ctxBag.Snapshot()
	if len(msgs) != 1 || msgs[0]["content"] != "Hello there." {
		t.Fatalf("context = %#v", msgs)
	}
}

func TestAssistantToolResultsFollowUpOnce(t *testing.T) {
	ctxBag := NewLLMContext("")
	a := NewAssistantAggregator("asst", ctxBag)
	var ups []string
	emit := processor.Emit{
		Down: func(frames.Frame) {},
		Up: func(f frames.Frame) {
			ups = append(ups, f.FrameKind())
		},
	}
	ctx := context.Background()
	off := false
	steps := []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "Checking"},
		&frames.FunctionCallsStartedFrame{Content: "Checking", Batch: []tools.Call{
			{ID: "c1", Name: "get_weather", Args: json.RawMessage(`{"location":"Paris"}`)},
			{ID: "c2", Name: "get_time", Args: json.RawMessage(`{}`)},
		}},
		&frames.LLMFullResponseEndFrame{},
		&frames.FunctionCallResultFrame{CallID: "c2", Name: "get_time", Result: "noon"},
		&frames.FunctionCallResultFrame{CallID: "c1", Name: "get_weather", Result: "sun"},
	}
	for _, f := range steps {
		if err := a.Process(ctx, f, processor.Downstream, emit); err != nil {
			t.Fatal(err)
		}
	}
	if len(ups) != 1 || ups[0] != frames.KindLLMRun {
		t.Fatalf("upstream = %v", ups)
	}
	msgs := ctxBag.Messages()
	if len(msgs) != 3 || msgs[0].Content != "Checking" || len(msgs[0].ToolCalls) != 2 {
		t.Fatalf("assistant = %#v", msgs)
	}
	if msgs[0].ToolCalls[0].ID != "c1" || msgs[1].ToolCallID != "c1" || msgs[1].Content != "sun" {
		t.Fatalf("order = %#v / %#v", msgs[1], msgs[2])
	}
	if msgs[2].ToolCallID != "c2" || msgs[2].Content != "noon" {
		t.Fatalf("second = %#v", msgs[2])
	}
	// A result that opts out of a follow-up does not ask for another run.
	a2 := NewAssistantAggregator("asst", NewLLMContext(""))
	ups = nil
	_ = a2.Process(ctx, &frames.FunctionCallsStartedFrame{Batch: []tools.Call{{ID: "c", Name: "n"}}}, processor.Downstream, emit)
	_ = a2.Process(ctx, &frames.FunctionCallResultFrame{CallID: "c", Result: "x", RunLLM: &off}, processor.Downstream, emit)
	if len(ups) != 0 {
		t.Fatalf("skip follow-up, got %v", ups)
	}
}

func TestAssistantCancelledToolDoesNotFollowUp(t *testing.T) {
	ctxBag := NewLLMContext("")
	a := NewAssistantAggregator("asst", ctxBag)
	var ups int
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) { ups++ }}
	ctx := context.Background()
	_ = a.Process(ctx, &frames.FunctionCallsStartedFrame{Batch: []tools.Call{{ID: "c", Name: "slow"}}}, processor.Downstream, emit)
	_ = a.Process(ctx, &frames.InterruptionFrame{}, processor.Downstream, emit)
	_ = a.Process(ctx, &frames.FunctionCallResultFrame{CallID: "c", Result: "CANCELLED", Cancelled: true}, processor.Downstream, emit)
	if ups != 0 {
		t.Fatalf("follow-ups = %d", ups)
	}
	msgs := ctxBag.Messages()
	if len(msgs) != 2 || msgs[1].Content != "CANCELLED" {
		t.Fatalf("context = %#v", msgs)
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
