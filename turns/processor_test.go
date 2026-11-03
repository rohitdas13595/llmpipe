package turns

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestProcessorEmitsUserFramesAndInterrupts(t *testing.T) {
	p := NewUserTurnProcessor("turns",
		[]StartStrategy{VADStart{}, TranscriptionStart{}, MinWordsStart{N: 1, WhenBotSpeaking: true}},
		[]StopStrategy{&SpeechTimeoutStop{}},
		nil,
	)
	var down, up []string
	emit := processor.Emit{
		Down: func(f frames.Frame) { down = append(down, f.FrameKind()) },
		Up:   func(f frames.Frame) { up = append(up, f.FrameKind()) },
	}
	ctx := context.Background()
	_ = p.Process(ctx, &frames.BotStartedSpeakingFrame{}, processor.Upstream, emit)
	_ = p.Process(ctx, &frames.VADUserStartedSpeakingFrame{}, processor.Downstream, emit)
	sawInterrupt := false
	for _, k := range up {
		if k == frames.KindInterruptionTask {
			sawInterrupt = true
		}
	}
	if !sawInterrupt {
		t.Fatalf("upstream %v", up)
	}
	if len(down) < 2 || down[0] != frames.KindUserStartedSpeaking || down[1] != frames.KindVADUserStartedSpeaking {
		t.Fatalf("downstream %v", down)
	}
	down = nil
	_ = p.Process(ctx, &frames.VADUserStoppedSpeakingFrame{}, processor.Downstream, emit)
	found := false
	for _, k := range down {
		if k == frames.KindUserStoppedSpeaking {
			found = true
		}
	}
	if !found {
		t.Fatalf("downstream %v", down)
	}
}

func TestProcessorDropsMutedAudio(t *testing.T) {
	p := NewUserTurnProcessor("turns",
		[]StartStrategy{VADStart{}},
		nil,
		[]MuteStrategy{&FirstSpeechMute{}},
	)
	var kinds []string
	emit := processor.Emit{Down: func(f frames.Frame) { kinds = append(kinds, f.FrameKind()) }}
	ctx := context.Background()
	audio := &frames.InputAudioRawFrame{Audio: []byte{1, 0, 2, 0}, SampleRate: 16000}
	_ = p.Process(ctx, &frames.VADUserStartedSpeakingFrame{}, processor.Downstream, emit)
	_ = p.Process(ctx, audio, processor.Downstream, emit)
	for _, k := range kinds {
		if k == frames.KindInputAudio || k == frames.KindUserStartedSpeaking {
			t.Fatalf("muted path emitted %v", kinds)
		}
	}
	_ = p.Process(ctx, &frames.BotStoppedSpeakingFrame{}, processor.Upstream, emit)
	kinds = nil
	_ = p.Process(ctx, &frames.VADUserStartedSpeakingFrame{}, processor.Downstream, emit)
	_ = p.Process(ctx, audio, processor.Downstream, emit)
	var started, heard bool
	for _, k := range kinds {
		if k == frames.KindUserStartedSpeaking {
			started = true
		}
		if k == frames.KindInputAudio {
			heard = true
		}
	}
	if !started || !heard {
		t.Fatalf("after unmute %v", kinds)
	}
}

func TestVADStopToUserStoppedUnder80ms(t *testing.T) {
	p := NewUserTurnProcessor("turns",
		[]StartStrategy{VADStart{}},
		[]StopStrategy{&SpeechTimeoutStop{}},
		nil,
	)
	var stopped atomicTime
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.UserStoppedSpeakingFrame); ok {
				stopped.mark()
			}
			return nil
		},
	}
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(p, tail))
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
	if err := task.QueueFrame(context.Background(), &frames.VADUserStartedSpeakingFrame{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	if err := task.QueueFrame(context.Background(), &frames.VADUserStoppedSpeakingFrame{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for !stopped.done() {
		if time.Now().After(deadline) {
			t.Fatal("no UserStopped")
		}
		time.Sleep(time.Millisecond)
	}
	if elapsed := stopped.when().Sub(start); elapsed > 80*time.Millisecond {
		t.Fatalf("VAD stop → UserStopped took %s", elapsed)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
	}
}

func TestSpeechTimeoutFiresThroughQueue(t *testing.T) {
	p := NewUserTurnProcessor("turns",
		[]StartStrategy{VADStart{}},
		[]StopStrategy{&SpeechTimeoutStop{Timeout: 30 * time.Millisecond}},
		nil,
	)
	var stopped atomic.Int32
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.UserStoppedSpeakingFrame); ok {
				stopped.Add(1)
			}
			return nil
		},
	}
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(p, tail))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.VADUserStartedSpeakingFrame{},
		&frames.VADUserStoppedSpeakingFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for stopped.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stopped %d", stopped.Load())
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

func TestSmartTurnResultComesBackOnTheProcessor(t *testing.T) {
	fake := &fakeAnalyzer{end: turn.Complete}
	p := NewUserTurnProcessor("turns",
		[]StartStrategy{VADStart{}},
		[]StopStrategy{&TurnAnalyzerStop{Analyzer: fake}},
		nil,
	)
	var stopped atomic.Int32
	tail := processor.Func{
		N: "tail",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.UserStoppedSpeakingFrame); ok {
				stopped.Add(1)
			}
			return nil
		},
	}
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(p, tail))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.VADUserStartedSpeakingFrame{},
		&frames.InputAudioRawFrame{Audio: []byte{1, 0, 2, 0}, SampleRate: 16000},
		&frames.VADUserStoppedSpeakingFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for stopped.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stopped %d", stopped.Load())
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

func waitRunning(t *testing.T, task *pipeline.PipelineTask) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

type atomicTime struct {
	ns atomic.Int64
}

func (a *atomicTime) mark() { a.ns.Store(time.Now().UnixNano()) }

func (a *atomicTime) done() bool { return a.ns.Load() != 0 }

func (a *atomicTime) when() time.Time { return time.Unix(0, a.ns.Load()) }
