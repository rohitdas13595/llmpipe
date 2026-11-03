package base

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
)

func pcmMS(ms, rate int) []byte {
	n := rate * ms / 1000 * 2
	if n < 2 {
		n = 2
	}
	return bytes.Repeat([]byte{1, 0}, n/2)
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestAudioOutputPacingAndInterrupt(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	var writes atomic.Int32
	var clears atomic.Int32
	o := &AudioOutput{
		SampleRate:  16000,
		ChunkMS:     40,
		Lead:        120 * time.Millisecond,
		StopSilence: 350 * time.Millisecond,
		Clock:       clock,
		Write: func(b []byte) error {
			writes.Add(int32(len(b)))
			return nil
		},
		Clear: func() error {
			clears.Add(1)
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx, nil)
	defer o.Stop()

	o.Enqueue(&frames.TTSAudioRawFrame{Audio: pcmMS(2000, 16000), SampleRate: 16000, NumChannels: 1})
	lead := 16000 * 120 / 1000 * 2
	chunk := 16000 * 40 / 1000 * 2
	waitUntil(t, 2*time.Second, func() bool { return o.BytesSent() >= lead })
	time.Sleep(30 * time.Millisecond)
	if got := o.BytesSent(); got > lead+chunk {
		t.Fatalf("wrote past the lead without clock advance: %d bytes", got)
	}

	clock.Advance(500 * time.Millisecond)
	want := 16000 * 620 / 1000 * 2
	waitUntil(t, 2*time.Second, func() bool { return o.BytesSent() >= want-chunk })
	played := o.Interrupt()
	if clears.Load() != 1 {
		t.Fatalf("Clear calls = %d, want 1", clears.Load())
	}
	const leadMS = 120
	if played < 500-leadMS || played > 500+leadMS {
		t.Fatalf("played = %dms, want 500 ± %d", played, leadMS)
	}
	after := writes.Load()
	time.Sleep(40 * time.Millisecond)
	if writes.Load() != after {
		t.Fatalf("audio written after interrupt: %d -> %d", after, writes.Load())
	}
}

func TestAudioOutputNaturalStop(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	o := &AudioOutput{
		SampleRate:  16000,
		ChunkMS:     40,
		Lead:        120 * time.Millisecond,
		StopSilence: 350 * time.Millisecond,
		Clock:       clock,
		Write:       func([]byte) error { return nil },
		Clear:       func() error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx, nil)
	defer o.Stop()

	o.Enqueue(&frames.TTSAudioRawFrame{Audio: pcmMS(200, 16000), SampleRate: 16000})
	waitUntil(t, time.Second, func() bool { return o.Speaking() })
	clock.Advance(80 * time.Millisecond)
	waitUntil(t, time.Second, func() bool { return o.BytesSent() >= 16000*200/1000*2 })
	clock.Advance(350 * time.Millisecond)
	waitUntil(t, time.Second, func() bool { return !o.Speaking() })
}

func TestInterruptToSilenceP95(t *testing.T) {
	audio := pcmMS(400, 16000)
	samples := make([]time.Duration, 0, 30)
	for i := 0; i < 30; i++ {
		var cleared atomic.Bool
		o := &AudioOutput{
			SampleRate: 16000,
			Lead:       20 * time.Millisecond,
			ChunkMS:    20,
			Write:      func([]byte) error { return nil },
			Clear: func() error {
				cleared.Store(true)
				return nil
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		o.Start(ctx, nil)
		o.Enqueue(&frames.TTSAudioRawFrame{Audio: audio, SampleRate: 16000})
		waitUntil(t, time.Second, func() bool { return o.BytesSent() > 0 })
		start := time.Now()
		o.Interrupt()
		samples = append(samples, time.Since(start))
		if !cleared.Load() {
			cancel()
			o.Stop()
			t.Fatal("Clear was not called")
		}
		o.Stop()
		cancel()
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p95 := samples[len(samples)*95/100]
	if p95 > 50*time.Millisecond {
		t.Fatalf("interrupt p95 = %s, want < 50ms", p95)
	}
}

func TestAudioOutputInterruptRace(t *testing.T) {
	audio := pcmMS(40, 16000)
	for i := 0; i < 100; i++ {
		o := &AudioOutput{
			SampleRate: 16000,
			Write:      func([]byte) error { return nil },
			Clear:      func() error { return nil },
		}
		ctx, cancel := context.WithCancel(context.Background())
		o.Start(ctx, nil)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				o.Enqueue(&frames.TTSAudioRawFrame{Audio: audio, SampleRate: 16000})
				o.EnqueueText(&frames.TTSTextFrame{Text: "hi", Meta: frames.Meta{PTS: 20 * time.Millisecond}})
			}
		}()
		go func() {
			defer wg.Done()
			o.Interrupt()
		}()
		wg.Wait()
		o.Stop()
		cancel()
	}
}

func BenchmarkInterruptToSilence(b *testing.B) {
	audio := pcmMS(200, 16000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		o := &AudioOutput{
			SampleRate: 16000,
			Write:      func([]byte) error { return nil },
			Clear:      func() error { return nil },
		}
		ctx, cancel := context.WithCancel(context.Background())
		o.Start(ctx, nil)
		o.Enqueue(&frames.TTSAudioRawFrame{Audio: audio, SampleRate: 16000})
		for o.BytesSent() == 0 {
			time.Sleep(100 * time.Microsecond)
		}
		o.Interrupt()
		o.Stop()
		cancel()
	}
}
