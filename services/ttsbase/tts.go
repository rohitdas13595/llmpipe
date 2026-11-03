// Package ttsbase turns a REST or websocket synthesizer into a pipeline
// processor. LLM text is aggregated into sentences, synthesized in order, and
// emitted as TTSStarted, audio, TTSTextFrame (with PTS), and TTSStopped.
// An interruption closes the active context so late audio is dropped.
package ttsbase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/textagg"
)

// Alignment is per-character timing relative to one audio chunk.
// StartMS and DurMS match the ElevenLabs charStartTimesMs / charDurationsMs
// arrays (milliseconds, chunk-relative).
type Alignment struct {
	Chars   []string
	StartMS []int
	DurMS   []int
}

// AudioChunk is one piece of synthesized PCM.
type AudioChunk struct {
	PCM        []byte
	SampleRate int
	Alignment  *Alignment
}

// Synthesizer produces a full utterance over HTTP (or any blocking API).
// Implementations call out once per chunk, in order, and return when the
// utterance is finished or ctx is cancelled.
type Synthesizer interface {
	Synthesize(ctx context.Context, text string, out func(AudioChunk) error) error
}

// StreamEvent is one message from a StreamingSynthesizer.
// Audio.PCM is empty when the event is only a final marker or an error.
type StreamEvent struct {
	ContextID string
	Audio     AudioChunk
	Final     bool
	Err       error
}

// StreamingSynthesizer keeps one websocket open and multiplexes contexts.
// Receive is closed when Close returns. Audio for a context passed to
// CloseContext must not be delivered.
type StreamingSynthesizer interface {
	Connect(ctx context.Context) error
	SendText(ctx context.Context, contextID, text string) error
	Flush(ctx context.Context, contextID string) error
	CloseContext(ctx context.Context, contextID string) error
	Receive() <-chan StreamEvent
	Close() error
}

// Options configures the processor wrapped around a synthesizer.
type Options struct {
	Name       string
	SampleRate int
	Aggregator textagg.Aggregator
	// PushTextFrames forces TTSTextFrame emission. The zero value still
	// emits them; set SuppressTextFrames to skip word/sentence PTS frames.
	PushTextFrames     bool
	SuppressTextFrames bool
}

// TTS is a pipeline processor. Construct it with New or NewStreaming.
type TTS struct {
	name       string
	sampleRate int
	agg        textagg.Aggregator
	pushText   bool
	synth      Synthesizer
	stream     StreamingSynthesizer

	services.Slot

	mu           sync.Mutex
	gen          int
	ctxID        string
	turnCtx      context.Context
	turnCancel   context.CancelFunc
	started      bool
	stopped      bool
	samples      int
	rate         int
	alignBaseMS  int
	chars        []timedChar
	pending      []string
	heldEnd      frames.Frame
	waitingFinal bool
	jobs         chan job
	stopCh       chan struct{}
	closed       bool
	workerWG     sync.WaitGroup
	recvWG       sync.WaitGroup
}

type timedChar struct {
	ch    string
	endMS int
}

type job struct {
	gen      int
	ctx      context.Context
	ctxID    string
	text     string
	end      bool
	endFrame frames.Frame
}

type spoken struct {
	text string
	pts  time.Duration
	id   string
}

// New wraps a request/response synthesizer. Sentences are synthesized one at
// a time, in the order the aggregator emits them.
func New(s Synthesizer, o Options) *TTS {
	return newTTS(s, nil, o)
}

// NewStreaming wraps a websocket synthesizer. Connect runs from Setup.
func NewStreaming(s StreamingSynthesizer, o Options) *TTS {
	return newTTS(nil, s, o)
}

func newTTS(s Synthesizer, stream StreamingSynthesizer, o Options) *TTS {
	if o.SampleRate <= 0 {
		o.SampleRate = 16000
	}
	if o.Aggregator == nil {
		o.Aggregator = textagg.NewSentenceAggregator()
	}
	if strings.TrimSpace(o.Name) == "" {
		o.Name = "tts"
	}
	push := true
	if o.SuppressTextFrames && !o.PushTextFrames {
		push = false
	}
	return &TTS{
		name:       o.Name,
		sampleRate: o.SampleRate,
		agg:        o.Aggregator,
		pushText:   push,
		synth:      s,
		stream:     stream,
		rate:       o.SampleRate,
	}
}

// Name implements processor.Processor.
func (t *TTS) Name() string { return t.name }

// Setup stores the pipeline handle and connects a streaming synthesizer.
func (t *TTS) Setup(ctx context.Context, h processor.Handle) error {
	if err := t.Slot.Setup(ctx, h); err != nil {
		return err
	}
	if t.stream == nil {
		return nil
	}
	t.recvWG.Add(1)
	go func() {
		defer t.recvWG.Done()
		if err := t.stream.Connect(h.Context()); err != nil {
			t.push(h.Context(), processor.Emit{}, &frames.ErrorFrame{Err: err})
			return
		}
		t.recv()
	}()
	return nil
}

// Cleanup stops the synth worker and closes a streaming connection.
func (t *TTS) Cleanup(ctx context.Context) error {
	t.stopWorker()
	if t.stream != nil {
		_ = t.stream.Close()
	}
	t.recvWG.Wait()
	if t.turnCancel != nil {
		t.turnCancel()
	}
	return nil
}

// Process aggregates LLM text and schedules synthesis.
func (t *TTS) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
	case *frames.LLMFullResponseStartFrame:
		emit.Down(f)
		t.begin(emit)
	case *frames.LLMTextFrame:
		t.feed(emit, fr.Text)
		emit.Down(f)
	case *frames.TextFrame:
		t.feed(emit, fr.Text)
		emit.Down(f)
	case *frames.LLMFullResponseEndFrame:
		t.finish(emit, f)
	case *frames.InterruptionFrame:
		t.interrupt(emit)
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (t *TTS) begin(emit processor.Emit) {
	t.mu.Lock()
	if t.turnCancel != nil {
		t.turnCancel()
	}
	parent := context.Background()
	if h := t.H(); h != nil && h.TurnContext() != nil {
		parent = h.TurnContext()
	}
	ctx, cancel := context.WithCancel(parent)
	t.gen++
	t.ctxID = newContextID()
	t.turnCtx = ctx
	t.turnCancel = cancel
	t.started = true
	t.stopped = false
	t.samples = 0
	t.alignBaseMS = 0
	t.chars = nil
	t.pending = nil
	t.heldEnd = nil
	t.waitingFinal = false
	t.rate = t.sampleRate
	t.mu.Unlock()
	if t.agg != nil {
		t.agg.Reset()
	}
	t.push(ctx, emit, &frames.TTSStartedFrame{})
}

func (t *TTS) feed(emit processor.Emit, text string) {
	if t.agg == nil || text == "" {
		return
	}
	for _, s := range t.agg.Add(text) {
		t.submit(emit, s, false, nil)
	}
}

func (t *TTS) finish(emit processor.Emit, end frames.Frame) {
	rest := ""
	if t.agg != nil {
		rest = strings.TrimSpace(t.agg.Flush())
	}
	t.submit(emit, rest, true, end)
}

func (t *TTS) submit(emit processor.Emit, text string, end bool, endFrame frames.Frame) {
	t.mu.Lock()
	j := job{
		gen:      t.gen,
		ctx:      t.turnCtx,
		ctxID:    t.ctxID,
		text:     strings.TrimSpace(text),
		end:      end,
		endFrame: endFrame,
	}
	async := t.H() != nil
	t.mu.Unlock()
	if j.ctx == nil {
		j.ctx = context.Background()
	}
	if j.text == "" && !j.end {
		return
	}
	if !async {
		t.runJob(emit, j)
		return
	}
	t.enqueue(j)
}

func (t *TTS) enqueue(j job) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	if t.jobs == nil {
		t.jobs = make(chan job, 32)
		t.stopCh = make(chan struct{})
		t.workerWG.Add(1)
		go t.loop()
	}
	ch := t.jobs
	stop := t.stopCh
	t.mu.Unlock()
	select {
	case <-stop:
	case ch <- j:
	}
}

func (t *TTS) loop() {
	defer t.workerWG.Done()
	for {
		select {
		case <-t.stopCh:
			return
		case j := <-t.jobs:
			t.runJob(processor.Emit{}, j)
		}
	}
}

func (t *TTS) stopWorker() {
	t.mu.Lock()
	t.closed = true
	stop := t.stopCh
	t.mu.Unlock()
	if stop != nil {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	t.workerWG.Wait()
}

func (t *TTS) runJob(emit processor.Emit, j job) {
	if j.gen != t.currentGen() {
		return
	}
	if j.text != "" {
		if t.stream != nil {
			t.mu.Lock()
			if t.gen == j.gen {
				t.pending = append(t.pending, j.text)
			}
			t.mu.Unlock()
			if err := t.stream.SendText(j.ctx, j.ctxID, j.text); err != nil && t.currentGen() == j.gen {
				t.push(j.ctx, emit, &frames.ErrorFrame{Err: err})
			}
		} else if t.synth != nil {
			t.speakREST(emit, j)
		}
	}
	if !j.end || t.currentGen() != j.gen {
		return
	}
	if t.stream != nil {
		t.mu.Lock()
		if t.gen == j.gen {
			t.heldEnd = j.endFrame
			t.waitingFinal = true
		}
		t.mu.Unlock()
		if err := t.stream.Flush(j.ctx, j.ctxID); err != nil && t.currentGen() == j.gen {
			t.push(j.ctx, emit, &frames.ErrorFrame{Err: err})
			t.completeStream(emit, j.gen)
		}
		return
	}
	t.completeREST(emit, j)
}

func (t *TTS) speakREST(emit processor.Emit, j job) {
	err := t.synth.Synthesize(j.ctx, j.text, func(ch AudioChunk) error {
		if j.ctx.Err() != nil || t.currentGen() != j.gen {
			return context.Canceled
		}
		if len(ch.PCM) == 0 {
			return nil
		}
		sr := ch.SampleRate
		if sr <= 0 {
			sr = t.sampleRate
		}
		t.notePCM(ch.PCM, sr)
		t.push(j.ctx, emit, &frames.TTSAudioRawFrame{
			Audio:       append([]byte(nil), ch.PCM...),
			SampleRate:  sr,
			NumChannels: 1,
		})
		return nil
	})
	if t.currentGen() != j.gen || j.ctx.Err() != nil {
		return
	}
	if err != nil {
		t.push(j.ctx, emit, &frames.ErrorFrame{Err: err})
		return
	}
	t.emitSpoken(emit, j.ctx, spoken{text: j.text, pts: t.clock(), id: j.ctxID})
}

func (t *TTS) completeREST(emit processor.Emit, j job) {
	if t.currentGen() != j.gen {
		return
	}
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
	t.push(j.ctx, emit, &frames.TTSStoppedFrame{})
	if j.endFrame != nil {
		t.push(j.ctx, emit, j.endFrame)
	}
}

func (t *TTS) recv() {
	ch := t.stream.Receive()
	if ch == nil {
		return
	}
	for ev := range ch {
		t.onEvent(ev)
	}
}

func (t *TTS) onEvent(ev StreamEvent) {
	t.mu.Lock()
	gen := t.gen
	id := t.ctxID
	stale := ev.ContextID != "" && ev.ContextID != id
	t.mu.Unlock()
	if stale {
		return
	}
	ctx := t.turn()
	if ev.Err != nil {
		if t.currentGen() == gen {
			t.push(ctx, processor.Emit{}, &frames.ErrorFrame{Err: ev.Err})
		}
		return
	}
	if len(ev.Audio.PCM) > 0 && t.currentGen() == gen {
		sr := ev.Audio.SampleRate
		if sr <= 0 {
			sr = t.sampleRate
		}
		t.mu.Lock()
		if t.gen == gen {
			t.absorbLocked(ev.Audio)
		}
		ready := t.takeReadyLocked(false)
		t.mu.Unlock()
		t.push(ctx, processor.Emit{}, &frames.TTSAudioRawFrame{
			Audio:       append([]byte(nil), ev.Audio.PCM...),
			SampleRate:  sr,
			NumChannels: 1,
		})
		for _, s := range ready {
			t.emitSpoken(processor.Emit{}, ctx, s)
		}
	}
	if ev.Final {
		t.completeStream(processor.Emit{}, gen)
	}
}

func (t *TTS) completeStream(emit processor.Emit, gen int) {
	t.mu.Lock()
	if t.gen != gen || t.stopped {
		t.mu.Unlock()
		return
	}
	ready := t.takeReadyLocked(true)
	end := t.heldEnd
	t.heldEnd = nil
	t.waitingFinal = false
	t.stopped = true
	ctx := t.turnCtx
	t.mu.Unlock()
	for _, s := range ready {
		t.emitSpoken(emit, ctx, s)
	}
	t.push(ctx, emit, &frames.TTSStoppedFrame{})
	if end != nil {
		t.push(ctx, emit, end)
	}
}

func (t *TTS) interrupt(emit processor.Emit) {
	t.mu.Lock()
	old := t.ctxID
	cancel := t.turnCancel
	t.turnCancel = nil
	started := t.started && !t.stopped
	t.gen++
	t.ctxID = ""
	t.started = false
	t.stopped = true
	t.pending = nil
	t.chars = nil
	t.heldEnd = nil
	t.waitingFinal = false
	t.mu.Unlock()
	if t.agg != nil {
		t.agg.Reset()
	}
	if cancel != nil {
		cancel()
	}
	if t.stream != nil && old != "" {
		cctx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		_ = t.stream.CloseContext(cctx, old)
		cancelClose()
	}
	if started {
		t.push(context.Background(), emit, &frames.TTSStoppedFrame{})
	}
}

func (t *TTS) absorbLocked(ch AudioChunk) {
	sr := ch.SampleRate
	if sr <= 0 {
		sr = t.sampleRate
	}
	if sr <= 0 {
		sr = 16000
	}
	pcmMS := 0
	if len(ch.PCM) >= 2 && sr > 0 {
		pcmMS = (len(ch.PCM) / 2) * 1000 / sr
	}
	chunkEnd := pcmMS
	if ch.Alignment != nil {
		for i, c := range ch.Alignment.Chars {
			start, dur := 0, 0
			if i < len(ch.Alignment.StartMS) {
				start = ch.Alignment.StartMS[i]
			}
			if i < len(ch.Alignment.DurMS) {
				dur = ch.Alignment.DurMS[i]
			}
			if start+dur > chunkEnd {
				chunkEnd = start + dur
			}
			t.chars = append(t.chars, timedChar{ch: c, endMS: t.alignBaseMS + start + dur})
		}
	}
	t.alignBaseMS += chunkEnd
	t.samples += len(ch.PCM) / 2
	t.rate = sr
}

func (t *TTS) takeReadyLocked(final bool) []spoken {
	var out []spoken
	for len(t.pending) > 0 {
		need := utf8.RuneCountInString(t.pending[0])
		if len(t.chars) < need && !final {
			break
		}
		pts := time.Duration(t.alignBaseMS) * time.Millisecond
		if need > 0 && len(t.chars) >= need {
			pts = time.Duration(t.chars[need-1].endMS) * time.Millisecond
			t.chars = append([]timedChar(nil), t.chars[need:]...)
		} else if t.rate > 0 && t.samples > 0 {
			pts = time.Duration(t.samples) * time.Second / time.Duration(t.rate)
			t.chars = nil
		}
		out = append(out, spoken{text: t.pending[0], pts: pts, id: t.ctxID})
		t.pending = t.pending[1:]
	}
	return out
}

func (t *TTS) emitSpoken(emit processor.Emit, ctx context.Context, s spoken) {
	if !t.pushText || strings.TrimSpace(s.text) == "" {
		return
	}
	t.push(ctx, emit, &frames.TTSTextFrame{
		Text:      s.text,
		ContextID: s.id,
		Meta:      frames.Meta{PTS: s.pts},
	})
}

func (t *TTS) notePCM(pcm []byte, sr int) {
	if sr <= 0 {
		sr = t.sampleRate
	}
	t.mu.Lock()
	t.samples += len(pcm) / 2
	t.rate = sr
	t.mu.Unlock()
}

func (t *TTS) clock() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rate <= 0 || t.samples <= 0 {
		return 0
	}
	return time.Duration(t.samples) * time.Second / time.Duration(t.rate)
}

func (t *TTS) currentGen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gen
}

func (t *TTS) turn() context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.turnCtx != nil {
		return t.turnCtx
	}
	return context.Background()
}

func (t *TTS) push(ctx context.Context, emit processor.Emit, f frames.Frame) {
	if f == nil {
		return
	}
	if h := t.H(); h != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		_ = h.PushFrameCtx(ctx, f, processor.Downstream)
		return
	}
	if emit.Down != nil {
		emit.Down(f)
	}
}

func newContextID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return hex.EncodeToString(b[:])
}
