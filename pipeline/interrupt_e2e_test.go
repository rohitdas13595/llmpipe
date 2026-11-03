package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/transport/base"
	"github.com/rohitdas13595/llmpipe/turns"
)

func TestOneInterruptionReachesEveryNode(t *testing.T) {
	var n [3]atomic.Int32
	mk := func(i int) processor.Processor {
		return processor.Func{
			N: fmt.Sprintf("n%d", i),
			F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
				if _, ok := f.(*frames.InterruptionFrame); ok {
					n[i].Add(1)
				}
				if tf, ok := f.(*frames.TextFrame); ok && tf.Text == "go" && i == 2 {
					emit.Up(&frames.InterruptionTaskFrame{})
				}
				processor.Forward(dir, f, emit)
				return nil
			},
		}
	}
	task := NewPipelineTask(NewPipeline(mk(0), mk(1), mk(2)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "go"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for n[0].Load() != 1 || n[1].Load() != 1 || n[2].Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("counts %d %d %d", n[0].Load(), n[1].Load(), n[2].Load())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not stop")
	}
}

type clockOut struct {
	ao *base.AudioOutput
}

func (c *clockOut) Name() string { return "out" }
func (c *clockOut) Setup(ctx context.Context, h processor.Handle) error {
	c.ao.Start(ctx, h)
	return nil
}
func (c *clockOut) Cleanup(context.Context) error {
	c.ao.Stop()
	return nil
}
func (c *clockOut) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	return c.ao.Handle(f, dir, emit)
}

func TestAssistantInterruptAt600msCommitsSentence1(t *testing.T) {
	clock := base.NewFakeClock(time.Unix(0, 0))
	var clears atomic.Int32
	ao := &base.AudioOutput{
		SampleRate:  16000,
		ChunkMS:     40,
		Lead:        120 * time.Millisecond,
		StopSilence: time.Hour,
		Clock:       clock,
		Write:       func([]byte) error { return nil },
		Clear: func() error {
			clears.Add(1)
			return nil
		},
	}
	ctxBag := aggregate.NewLLMContext("")
	asst := aggregate.NewAssistantAggregator("asst", ctxBag)
	task := NewPipelineTask(NewPipeline(&clockOut{ao: ao}, asst))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)

	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.TTSStartedFrame{},
		&frames.TTSTextFrame{Text: "Hello there.", Meta: frames.Meta{PTS: 500 * time.Millisecond}},
		&frames.TTSTextFrame{Text: "Goodbye now.", Meta: frames.Meta{PTS: 1500 * time.Millisecond}},
		&frames.TTSAudioRawFrame{Audio: bytes.Repeat([]byte{1, 0}, 16000*2), SampleRate: 16000, NumChannels: 1},
		&frames.LLMFullResponseEndFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	lead := 16000 * 120 / 1000 * 2
	waitUntil(t, func() bool { return ao.BytesSent() >= lead })
	clock.Advance(600 * time.Millisecond)
	want := 16000 * 720 / 1000 * 2
	waitUntil(t, func() bool { return ao.BytesSent() >= want-16000*40/1000*2 })
	if err := task.QueueFrame(context.Background(), &frames.InterruptionFrame{}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		msgs := ctxBag.Snapshot()
		return len(msgs) == 1 && msgs[0]["content"] == "Hello there."
	})
	if clears.Load() != 1 {
		t.Fatalf("clears = %d", clears.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not stop")
	}
}

type holdLLM struct {
	services.Slot
	cancelled chan struct{}
	once      sync.Once
}

func (h *holdLLM) Name() string { return "llm" }
func (h *holdLLM) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	if _, ok := f.(*frames.LLMRunFrame); ok {
		turn := h.TurnContext()
		go func() {
			push := func(ff frames.Frame) {
				if h.H() != nil {
					_ = h.H().PushFrameCtx(turn, ff, processor.Downstream)
				}
			}
			push(&frames.LLMFullResponseStartFrame{})
			push(&frames.LLMTextFrame{Text: "Hello there. And more words."})
			push(&frames.LLMFullResponseEndFrame{})
			select {
			case <-turn.Done():
				h.once.Do(func() { close(h.cancelled) })
			case <-time.After(3 * time.Second):
			}
		}()
		return nil
	}
	emit.Down(f)
	return nil
}

type scriptTTS struct{}

func (scriptTTS) Name() string { return "tts" }
func (scriptTTS) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	if _, ok := f.(*frames.LLMFullResponseEndFrame); ok {
		emit.Down(&frames.TTSStartedFrame{})
		emit.Down(&frames.TTSTextFrame{Text: "Hello there.", Meta: frames.Meta{PTS: 0}})
		emit.Down(&frames.TTSTextFrame{Text: "And more words.", Meta: frames.Meta{PTS: 5 * time.Second}})
		emit.Down(&frames.TTSAudioRawFrame{
			Audio:       bytes.Repeat([]byte{1, 0}, 16000*2),
			SampleRate:  16000,
			NumChannels: 1,
		})
		emit.Down(&frames.TTSStoppedFrame{})
		emit.Down(f)
		return nil
	}
	emit.Down(f)
	return nil
}

func TestE2EInterruptWithin150ms(t *testing.T) {
	bot := aggregate.NewBotState()
	ctxBag := aggregate.NewLLMContext("")
	user := aggregate.NewUserAggregator("user", ctxBag, bot, interrupt.MinWords{N: 1})
	llm := &holdLLM{cancelled: make(chan struct{})}
	var writes atomic.Int32
	var cleared atomic.Int64
	ao := &base.AudioOutput{
		SampleRate:  16000,
		ChunkMS:     20,
		Lead:        40 * time.Millisecond,
		StopSilence: time.Hour,
		Write: func([]byte) error {
			writes.Add(1)
			return nil
		},
		Clear: func() error {
			cleared.CompareAndSwap(0, time.Now().UnixNano())
			return nil
		},
	}
	asst := aggregate.NewAssistantAggregator("asst", ctxBag)
	ut := turns.NewUserTurnProcessor("turns", []turns.StartStrategy{
		turns.TranscriptionStart{},
		turns.MinWordsStart{N: 1, WhenBotSpeaking: true},
	}, nil, nil)
	task := NewPipelineTask(NewPipeline(ut, user, llm, scriptTTS{}, &clockOut{ao: ao}, asst))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrame(context.Background(), &frames.LLMRunFrame{}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return bot.Speaking() })
	start := time.Now()
	if err := task.QueueFrame(context.Background(), &frames.InterimTranscriptionFrame{Text: "hey"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return cleared.Load() != 0 })
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("clear took %s, want <= 150ms", elapsed)
	}
	select {
	case <-llm.cancelled:
	case <-time.After(time.Second):
		t.Fatal("llm turn was not cancelled")
	}
	waitUntil(t, func() bool {
		msgs := ctxBag.Snapshot()
		for _, m := range msgs {
			if m["role"] == "assistant" && m["content"] == "Hello there." {
				return true
			}
		}
		return false
	})
	for _, m := range ctxBag.Snapshot() {
		if m["role"] == "assistant" && m["content"] != "Hello there." {
			t.Fatalf("assistant context = %#v", m["content"])
		}
	}
	stable := writes.Load()
	time.Sleep(50 * time.Millisecond)
	if writes.Load() != stable {
		t.Fatalf("audio writes after clear: %d -> %d", stable, writes.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not stop")
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
