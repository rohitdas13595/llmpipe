// Package local provides PCM streaming over generic io.Reader / io.Writer (no platform audio capture).
// For microphone/speaker devices use an external bridge or wrap a host-specific reader/writer.
package local

import (
	"context"
	"io"
	"sync"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/transport/base"
)

// Transport pumps PCM16 LE mono from In and writes TTS PCM to Out.
type Transport struct {
	SampleRate int
	// In is optional; if nil, StartInput is a no-op for reading.
	In io.Reader
	// Out is optional; if nil, TTS output is discarded.
	Out io.Writer

	queue func(context.Context, []frames.Frame) error
	stop  func()
	mu    sync.Mutex
}

// NewTransport builds a local I/O transport. chunkBytes is read size per loop (default ~20ms mono 16-bit).
func NewTransport(sampleRate int, in io.Reader, out io.Writer, queue func(context.Context, []frames.Frame) error) *Transport {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &Transport{SampleRate: sampleRate, In: in, Out: out, queue: queue}
}

// defaultChunk returns ~20ms of mono s16le PCM bytes.
func (t *Transport) defaultChunk() int {
	n := t.SampleRate / 50 * 2
	if n < 320 {
		n = 320
	}
	return n
}

// StartInput reads In until EOF or ctx done, queuing InputAudioRawFrame chunks. Idempotent.
func (t *Transport) StartInput(ctx context.Context) {
	t.mu.Lock()
	if t.stop != nil {
		t.mu.Unlock()
		return
	}
	if t.In == nil || t.queue == nil {
		t.mu.Unlock()
		return
	}
	sctx, cancel := context.WithCancel(ctx)
	t.stop = cancel
	t.mu.Unlock()

	_ = t.queue(sctx, []frames.Frame{
		&frames.StartFrame{SampleRate: t.SampleRate, NumChannels: 1},
	})

	buf := make([]byte, t.defaultChunk())
	go func() {
		defer cancel()
		for {
			select {
			case <-sctx.Done():
				return
			default:
			}
			n, err := t.In.Read(buf)
			if n > 0 {
				p := make([]byte, n)
				copy(p, buf[:n])
				_ = t.queue(context.Background(), []frames.Frame{
					&frames.InputAudioRawFrame{Audio: p, SampleRate: t.SampleRate, NumChannels: 1},
				})
			}
			if err != nil {
				return
			}
		}
	}()
}

// StopInput cancels the reader started by StartInput.
func (t *Transport) StopInput() {
	t.mu.Lock()
	if t.stop != nil {
		t.stop()
		t.stop = nil
	}
	t.mu.Unlock()
}

// Input is a passthrough processor.
func (t *Transport) Input() processor.Processor {
	return processor.Func{
		N: "local.input",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			emit.Down(f)
			return nil
		},
	}
}

// Output paces TTS audio onto Out. Clear is a no-op: the writer has no
// client jitter buffer of its own.
func (t *Transport) Output() processor.Processor {
	ao := &base.AudioOutput{
		SampleRate: t.SampleRate,
		Write: func(b []byte) error {
			if t.Out == nil || len(b) == 0 {
				return nil
			}
			_, err := t.Out.Write(b)
			return err
		},
		Clear: func() error { return nil },
	}
	return &localOut{ao: ao}
}

type localOut struct {
	ao *base.AudioOutput
}

func (o *localOut) Name() string { return "local.output" }

func (o *localOut) Setup(ctx context.Context, h processor.Handle) error {
	o.ao.Start(ctx, h)
	return nil
}

func (o *localOut) Cleanup(context.Context) error {
	o.ao.Stop()
	return nil
}

func (o *localOut) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	return o.ao.Handle(f, dir, emit)
}
