package pipeline

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func BenchmarkHop(b *testing.B) {
	var n atomic.Int64
	p := processor.Func{
		N: "hop",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				n.Add(1)
			}
			emit.Down(f)
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	for !task.Running() {
		select {
		case err := <-done:
			b.Fatal(err)
		default:
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "h"}); err != nil {
			b.Fatal(err)
		}
	}
	for n.Load() < int64(b.N) {
	}
	b.StopTimer()
	task.StopWhenDone()
	<-done
}

func BenchmarkPipeline10Audio20ms(b *testing.B) {
	var n atomic.Int64
	procs := make([]processor.Processor, 10)
	for i := 0; i < 10; i++ {
		idx := i
		procs[i] = processor.Func{
			N: "s",
			F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
				if idx == 9 {
					if _, ok := f.(*frames.InputAudioRawFrame); ok {
						n.Add(1)
					}
				}
				emit.Down(f)
				return nil
			},
		}
	}
	task := NewPipelineTask(NewPipeline(procs...), WithTaskOptions(TaskOptions{
		// Input audio lives on the system lane and is drop-oldest past the cap.
		// The benchmark wants every frame to arrive.
		AudioSysQueueMax: b.N + 8,
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	for !task.Running() {
		select {
		case err := <-done:
			b.Fatal(err)
		default:
		}
	}
	pcm := make([]byte, 16000*2*20/1000) // 20ms mono s16le @ 16kHz
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := task.QueueFrame(context.Background(), &frames.InputAudioRawFrame{
			Audio: pcm, SampleRate: 16000, NumChannels: 1,
		}); err != nil {
			b.Fatal(err)
		}
	}
	for n.Load() < int64(b.N) {
	}
	b.StopTimer()
	task.StopWhenDone()
	<-done
}

func BenchmarkLegacyVsQueued(b *testing.B) {
	// The recursive engine is gone; this benchmark is the queued hop cost
	// kept under the name from the phase-1 plan so benchstat history lines up.
	BenchmarkHop(b)
}
