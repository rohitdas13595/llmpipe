package base

import (
	"context"
	"sync"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

const (
	defaultChunkMS     = 40
	defaultLead        = 120 * time.Millisecond
	defaultStopSilence = 350 * time.Millisecond
	defaultBotEvery    = 200 * time.Millisecond
)

// PlaybackTick asks the output processor to emit bot and played-text frames
// on its own goroutine. It is a system frame so it is not stuck behind audio
// data and is not dropped by an interruption drain.
type PlaybackTick struct {
	frames.System
	frames.Meta
}

// FrameKind implements frames.Frame.
func (PlaybackTick) FrameKind() string { return "PlaybackTick" }

type emitItem struct {
	f    frames.Frame
	both bool
}

type heldText struct {
	f        *frames.TTSTextFrame
	released bool
}

// AudioOutput paces PCM to a transport. It writes ChunkMS slices while
// staying Lead ahead of the clock, emits bot speaking frames for real
// playback, and releases TTSTextFrame values once heard time reaches PTS.
// Interrupt reports how many milliseconds the listener had heard, drops
// audio still in the buffer, and calls Clear once.
type AudioOutput struct {
	SampleRate  int
	ChunkMS     int
	Lead        time.Duration
	StopSilence time.Duration
	BotEvery    time.Duration
	Write       func([]byte) error
	Clear       func() error
	Clock       Clock

	mu         sync.Mutex
	h          processor.Handle
	buf        []byte
	rate       int
	texts      []heldText
	pending    []emitItem
	sent       int
	playStart  time.Time
	speaking   bool
	lastBot    time.Time
	drainedAt  time.Time
	gen        int
	loopOn     bool
	stopped    bool
	tickQueued bool
	stopCh     chan struct{}
	wakeCh     chan struct{}
	done       chan struct{}
}

// Start launches the pacer. h may be nil in tests that only inspect bytes.
func (o *AudioOutput) Start(ctx context.Context, h processor.Handle) {
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	if o.loopOn {
		o.mu.Unlock()
		return
	}
	o.applyDefaultsLocked()
	o.h = h
	o.loopOn = true
	o.stopped = false
	o.stopCh = make(chan struct{})
	o.wakeCh = make(chan struct{}, 1)
	o.done = make(chan struct{})
	o.mu.Unlock()
	go func() {
		defer close(o.done)
		o.loop(ctx)
	}()
}

// Stop ends the pacer and waits for it to return.
func (o *AudioOutput) Stop() {
	o.mu.Lock()
	if !o.loopOn {
		o.mu.Unlock()
		return
	}
	if o.stopped {
		done := o.done
		o.mu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	o.stopped = true
	done := o.done
	stop := o.stopCh
	o.mu.Unlock()
	close(stop)
	o.wake()
	<-done
}

// Enqueue appends PCM. Bytes are copied.
func (o *AudioOutput) Enqueue(a *frames.TTSAudioRawFrame) {
	if a == nil || len(a.Audio) == 0 {
		return
	}
	o.mu.Lock()
	o.buf = append(o.buf, a.Audio...)
	if a.SampleRate > 0 {
		o.rate = a.SampleRate
	}
	o.drainedAt = time.Time{}
	o.mu.Unlock()
	o.wake()
}

// EnqueueText holds t until playback reaches t.PTS, then the processor
// emits it downstream.
func (o *AudioOutput) EnqueueText(t *frames.TTSTextFrame) {
	if t == nil {
		return
	}
	o.mu.Lock()
	o.texts = append(o.texts, heldText{f: t})
	o.releaseTextLocked(o.heardLocked(o.nowLocked()))
	o.mu.Unlock()
	o.requestTick()
	o.wake()
}

// Interrupt drops buffered audio, calls Clear once, and reports heard
// milliseconds. Played text and bot-stopped frames are queued for Handle
// to emit before it forwards the InterruptionFrame.
func (o *AudioOutput) Interrupt() int {
	o.mu.Lock()
	now := o.nowLocked()
	played := o.heardLocked(now)
	ms := durMS(played)
	o.gen++
	o.buf = nil
	if o.speaking {
		o.releaseTextLocked(played)
		o.dropUnreleasedLocked()
		o.pending = append(o.pending, emitItem{
			f:    &frames.BotStoppedSpeakingFrame{PlayedMS: ms, Interrupted: true},
			both: true,
		})
		o.speaking = false
		o.playStart = time.Time{}
		o.sent = 0
		o.drainedAt = time.Time{}
	} else {
		o.texts = nil
	}
	o.pending = append(o.pending, emitItem{
		f: &frames.OutputTransportMessageUrgentFrame{Data: frames.TransportClearPayload},
	})
	o.mu.Unlock()
	o.wake()
	if o.Clear != nil {
		_ = o.Clear()
	}
	return ms
}

// BytesSent is the number of PCM bytes handed to Write in the current
// playback. It resets when playback stops.
func (o *AudioOutput) BytesSent() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sent
}

// Speaking reports whether the pacer has started playback and not yet stopped.
func (o *AudioOutput) Speaking() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.speaking
}

// Handle is the processor body shared by websocket, local, and LiveKit outputs.
func (o *AudioOutput) Handle(f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
	case *PlaybackTick:
		o.mu.Lock()
		o.tickQueued = false
		o.mu.Unlock()
		o.emitPending(emit)
		return nil
	case *frames.TTSAudioRawFrame:
		if !o.running() {
			return o.writeSync(fr, emit)
		}
		o.Enqueue(fr)
		return nil
	case *frames.TTSTextFrame:
		if !o.running() {
			processor.Forward(dir, f, emit)
			return nil
		}
		o.EnqueueText(fr)
		return nil
	case *frames.InterruptionFrame:
		o.Interrupt()
		o.emitPending(emit)
		processor.Forward(dir, f, emit)
		return nil
	default:
		processor.Forward(dir, f, emit)
		return nil
	}
}

func (o *AudioOutput) running() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.loopOn
}

func (o *AudioOutput) writeSync(fr *frames.TTSAudioRawFrame, emit processor.Emit) error {
	if o.Write != nil && len(fr.Audio) > 0 {
		if err := o.Write(fr.Audio); err != nil && emit.Down != nil {
			emit.Down(&frames.ErrorFrame{Err: err})
		}
	}
	processor.Forward(processor.Downstream, fr, emit)
	return nil
}

func (o *AudioOutput) emitPending(emit processor.Emit) {
	o.mu.Lock()
	items := o.pending
	o.pending = nil
	o.mu.Unlock()
	// Downstream first. Played text has to be queued before BotStarted goes
	// upstream, or a barge-in can commit before the assistant sees the words.
	var up []frames.Frame
	for _, it := range items {
		processor.Forward(processor.Downstream, it.f, emit)
		if it.both {
			up = append(up, it.f)
		}
	}
	for _, f := range up {
		processor.Forward(processor.Upstream, f, emit)
	}
}

func (o *AudioOutput) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		o.mu.Lock()
		if o.stopped {
			o.mu.Unlock()
			return
		}
		now := o.nowLocked()
		if len(o.buf) == 0 && o.speaking {
			if o.drainedAt.IsZero() {
				o.drainedAt = now
			}
			if now.Sub(o.drainedAt) >= o.StopSilence {
				o.finishNaturalLocked()
				o.mu.Unlock()
				o.requestTick()
				continue
			}
		}
		if len(o.buf) > 0 {
			elapsed := time.Duration(0)
			if !o.playStart.IsZero() {
				elapsed = now.Sub(o.playStart)
			}
			allowed := o.durToBytes(elapsed + o.Lead)
			if o.sent < allowed {
				n := allowed - o.sent
				chunk := o.chunkBytes()
				if n > chunk {
					n = chunk
				}
				if n > len(o.buf) {
					n = len(o.buf)
				}
				if n%2 == 1 {
					n--
				}
				if n > 0 {
					b := make([]byte, n)
					copy(b, o.buf[:n])
					rest := make([]byte, len(o.buf)-n)
					copy(rest, o.buf[n:])
					o.buf = rest
					gen := o.gen
					o.mu.Unlock()
					o.writeChunk(b, gen)
					continue
				}
			}
			target := o.sent + o.chunkBytes()
			need := o.bytesToDur(target) - o.Lead - elapsed
			if need < time.Millisecond {
				need = time.Millisecond
			}
			deadline := now.Add(need)
			o.mu.Unlock()
			if !o.waitUntil(ctx, deadline) {
				return
			}
			continue
		}
		deadline := now.Add(time.Hour)
		if o.speaking && !o.drainedAt.IsZero() {
			remain := o.StopSilence - now.Sub(o.drainedAt)
			if remain < time.Millisecond {
				remain = time.Millisecond
			}
			deadline = now.Add(remain)
		}
		o.mu.Unlock()
		if !o.waitUntil(ctx, deadline) {
			return
		}
	}
}

func (o *AudioOutput) writeChunk(b []byte, gen int) {
	o.mu.Lock()
	ok := gen == o.gen && !o.stopped
	write := o.Write
	o.mu.Unlock()
	if !ok || write == nil {
		return
	}
	if err := write(b); err != nil {
		o.noteWriteErr(err, gen)
		o.requestTick()
		return
	}
	o.noteWritten(b, gen)
	o.requestTick()
}

func (o *AudioOutput) noteWriteErr(err error, gen int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if gen != o.gen {
		return
	}
	o.pending = append(o.pending, emitItem{f: &frames.ErrorFrame{Err: err}})
}

func (o *AudioOutput) noteWritten(b []byte, gen int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if gen != o.gen {
		return
	}
	now := o.nowLocked()
	if !o.speaking {
		o.speaking = true
		o.playStart = now
		o.lastBot = now
		o.drainedAt = time.Time{}
		o.pending = append(o.pending, emitItem{f: &frames.BotStartedSpeakingFrame{}, both: true})
	}
	o.sent += len(b)
	if o.BotEvery > 0 && now.Sub(o.lastBot) >= o.BotEvery {
		o.lastBot = now
		o.pending = append(o.pending, emitItem{f: &frames.BotSpeakingFrame{}, both: true})
	}
	o.releaseTextLocked(o.heardLocked(now))
}

func (o *AudioOutput) finishNaturalLocked() {
	if !o.speaking {
		return
	}
	for i := range o.texts {
		if o.texts[i].released {
			continue
		}
		o.texts[i].released = true
		o.pending = append(o.pending, emitItem{f: o.texts[i].f})
	}
	ms := durMS(o.bytesToDur(o.sent))
	o.pending = append(o.pending, emitItem{
		f:    &frames.BotStoppedSpeakingFrame{PlayedMS: ms},
		both: true,
	})
	o.speaking = false
	o.playStart = time.Time{}
	o.sent = 0
	o.drainedAt = time.Time{}
}

func (o *AudioOutput) releaseTextLocked(heard time.Duration) {
	for i := range o.texts {
		if o.texts[i].released || o.texts[i].f == nil {
			continue
		}
		if o.texts[i].f.PTS <= heard {
			o.texts[i].released = true
			o.pending = append(o.pending, emitItem{f: o.texts[i].f})
		}
	}
}

func (o *AudioOutput) dropUnreleasedLocked() {
	kept := o.texts[:0]
	for _, t := range o.texts {
		if t.released {
			kept = append(kept, t)
		}
	}
	o.texts = kept
}

func (o *AudioOutput) heardLocked(now time.Time) time.Duration {
	if o.playStart.IsZero() {
		return 0
	}
	elapsed := now.Sub(o.playStart)
	if elapsed < 0 {
		elapsed = 0
	}
	written := o.bytesToDur(o.sent)
	if elapsed > written {
		return written
	}
	return elapsed
}

func (o *AudioOutput) waitUntil(ctx context.Context, deadline time.Time) bool {
	for {
		remain := deadline.Sub(o.clock().Now())
		if remain <= 0 {
			return true
		}
		timer := o.clock().NewTimer(remain)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-o.stopCh:
			timer.Stop()
			return false
		case <-o.wakeCh:
			timer.Stop()
			return true
		case <-timer.C():
			timer.Stop()
			return true
		}
	}
}

func (o *AudioOutput) wake() {
	select {
	case o.wakeCh <- struct{}{}:
	default:
	}
}

func (o *AudioOutput) requestTick() {
	o.mu.Lock()
	h := o.h
	if h == nil || o.tickQueued || o.stopped {
		o.mu.Unlock()
		return
	}
	o.tickQueued = true
	o.mu.Unlock()
	_ = h.QueueSelf(&PlaybackTick{})
}

func (o *AudioOutput) applyDefaultsLocked() {
	if o.SampleRate <= 0 {
		o.SampleRate = 16000
	}
	if o.rate <= 0 {
		o.rate = o.SampleRate
	}
	if o.ChunkMS <= 0 {
		o.ChunkMS = defaultChunkMS
	}
	if o.Lead <= 0 {
		o.Lead = defaultLead
	}
	if o.StopSilence <= 0 {
		o.StopSilence = defaultStopSilence
	}
	if o.BotEvery <= 0 {
		o.BotEvery = defaultBotEvery
	}
}

func (o *AudioOutput) nowLocked() time.Time { return o.clock().Now() }

func (o *AudioOutput) clock() Clock {
	if o.Clock != nil {
		return o.Clock
	}
	return realClock{}
}

func (o *AudioOutput) chunkBytes() int {
	n := o.durToBytes(time.Duration(o.ChunkMS) * time.Millisecond)
	if n < 2 {
		n = 2
	}
	return n
}

func (o *AudioOutput) durToBytes(d time.Duration) int {
	if d <= 0 || o.rate <= 0 {
		return 0
	}
	n := int(int64(o.rate) * int64(d) / int64(time.Second) * 2)
	if n%2 == 1 {
		n--
	}
	return n
}

func (o *AudioOutput) bytesToDur(n int) time.Duration {
	if n <= 0 || o.rate <= 0 {
		return 0
	}
	samples := n / 2
	return time.Duration(samples) * time.Second / time.Duration(o.rate)
}

func durMS(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d / time.Millisecond)
}
