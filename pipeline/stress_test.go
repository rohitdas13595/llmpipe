package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// TestStressEightPushers checks that one processor is never in Process on
// two goroutines, including while audio frames are queued beside data.
func TestStressEightPushers(t *testing.T) {
	const pushers = 8
	const each = 200
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	var sinkN atomic.Int32
	proc := processor.Func{
		N: "probe",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			n := inFlight.Add(1)
			for {
				old := maxSeen.Load()
				if n <= old || maxSeen.CompareAndSwap(old, n) {
					break
				}
			}
			inFlight.Add(-1)
			emit.Down(f)
			return nil
		},
	}
	sink := processor.Func{
		N: "sink",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			switch f.(type) {
			case *frames.TextFrame, *frames.InputAudioRawFrame:
				sinkN.Add(1)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(proc, sink), WithTaskOptions(TaskOptions{
		AudioSysQueueMax: pushers * each,
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)

	var wg sync.WaitGroup
	errCh := make(chan error, pushers)
	for p := 0; p < pushers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				var f frames.Frame
				if i%5 == 0 {
					f = &frames.InputAudioRawFrame{Audio: []byte{byte(id), byte(i)}, SampleRate: 16000, NumChannels: 1}
				} else {
					f = &frames.TextFrame{Text: "x"}
				}
				if err := task.QueueFrame(context.Background(), f); err != nil {
					errCh <- err
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	task.StopWhenDone()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stress timeout")
	}
	if maxSeen.Load() != 1 {
		t.Fatalf("in-flight max %d", maxSeen.Load())
	}
	if sinkN.Load() != pushers*each {
		t.Fatalf("sink saw %d", sinkN.Load())
	}
}

// TestRaceInFlight is the same probe under a name the CI stress filter matches.
func TestRaceInFlight(t *testing.T) {
	TestStressEightPushers(t)
}
