package aggregate

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
	"github.com/rohitdas13595/llmpipe/processor"
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

func TestUserInterimInterruptReachesTailOnce(t *testing.T) {
	ctxBag := NewLLMContext("")
	bot := NewBotState()
	bot.SetSpeaking(true)
	u := NewUserAggregator("ua", ctxBag, bot, interrupt.MinWords{N: 1})
	var interrupts int
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.InterruptionFrame); ok {
				interrupts++
			}
			return nil
		},
	}
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(u, tail))
	pipelinetest.UntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.InterimTranscriptionFrame{Text: "hey"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if interrupts != 1 {
		t.Fatalf("interruptions = %d, want 1", interrupts)
	}
}

func TestUserVADInterruptAfter300ms(t *testing.T) {
	bot := NewBotState()
	bot.SetSpeaking(true)
	u := NewUserAggregator("ua", NewLLMContext(""), bot, interrupt.MinWords{N: 1})
	var interrupts atomic.Int32
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.InterruptionFrame); ok {
				interrupts.Add(1)
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
	audio := bytes.Repeat([]byte{1, 0}, 16000*200/1000)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.UserStartedSpeakingFrame{},
		&frames.InputAudioRawFrame{Audio: audio, SampleRate: 16000, NumChannels: 1},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if interrupts.Load() != 0 {
		t.Fatalf("interrupted before 300ms: %d", interrupts.Load())
	}
	if err := task.QueueFrame(context.Background(), &frames.InputAudioRawFrame{Audio: audio, SampleRate: 16000, NumChannels: 1}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for interrupts.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("interrupts = %d", interrupts.Load())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
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
