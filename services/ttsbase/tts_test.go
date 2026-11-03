package ttsbase

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
	"github.com/rohitdas13595/llmpipe/processor"
)

type synthFunc func(ctx context.Context, text string, out func(AudioChunk) error) error

func (f synthFunc) Synthesize(ctx context.Context, text string, out func(AudioChunk) error) error {
	return f(ctx, text, out)
}

func TestRESTOrderedPaired(t *testing.T) {
	var mu sync.Mutex
	inFlight := 0
	var order []string
	s := synthFunc(func(ctx context.Context, text string, out func(AudioChunk) error) error {
		mu.Lock()
		if inFlight != 0 {
			mu.Unlock()
			t.Errorf("overlapping synth for %q", text)
			return nil
		}
		inFlight++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		err := out(AudioChunk{PCM: []byte{1, 0, 2, 0}, SampleRate: 16000})
		mu.Lock()
		inFlight--
		order = append(order, text)
		mu.Unlock()
		return err
	})
	tts := New(s, Options{Name: "tts", SampleRate: 16000, PushTextFrames: true})
	var kinds []string
	var texts []string
	var audio int
	emit := processor.Emit{Down: func(f frames.Frame) {
		kinds = append(kinds, f.FrameKind())
		switch fr := f.(type) {
		case *frames.TTSTextFrame:
			texts = append(texts, fr.Text)
			if fr.Meta.PTS <= 0 {
				t.Errorf("PTS for %q is %s", fr.Text, fr.Meta.PTS)
			}
		case *frames.TTSAudioRawFrame:
			audio++
		}
	}, Up: func(frames.Frame) {}}
	ctx := context.Background()
	_ = tts.Process(ctx, &frames.LLMFullResponseStartFrame{}, processor.Downstream, emit)
	_ = tts.Process(ctx, &frames.LLMTextFrame{Text: "Hello there. And more."}, processor.Downstream, emit)
	_ = tts.Process(ctx, &frames.LLMFullResponseEndFrame{}, processor.Downstream, emit)
	if audio != 2 {
		t.Fatalf("audio frames %d, kinds %v", audio, kinds)
	}
	if len(texts) != 2 || texts[0] != "Hello there." || texts[1] != "And more." {
		t.Fatalf("texts %v", texts)
	}
	started, stopped := 0, 0
	for _, k := range kinds {
		if k == frames.KindTTSStarted {
			started++
		}
		if k == frames.KindTTSStopped {
			stopped++
		}
	}
	if started != 1 || stopped != 1 {
		t.Fatalf("started %d stopped %d (%v)", started, stopped, kinds)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "Hello there." || order[1] != "And more." {
		t.Fatalf("order %v", order)
	}
}

func TestRESTCancelDropsAudio(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	s := synthFunc(func(ctx context.Context, text string, out func(AudioChunk) error) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})
	tts := New(s, Options{Name: "tts", SampleRate: 16000})
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(tts, rec.Processor("rec")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "Hello there. Next"},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("synth did not start")
	}
	if err := task.QueueFrame(context.Background(), &frames.InterruptionFrame{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !saw(rec, frames.KindInterruption) {
		if time.Now().After(deadline) {
			t.Fatalf("frames %s", kindsOf(rec))
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	for _, f := range rec.Frames() {
		if _, ok := f.(*frames.TTSAudioRawFrame); ok {
			t.Fatal("audio emitted after cancel")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
	}
}

func TestStreamingContextOrderAndStaleDrop(t *testing.T) {
	script := newScripted()
	tts := NewStreaming(script, Options{Name: "tts", SampleRate: 16000, PushTextFrames: true})
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(tts, rec.Processor("rec")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "Hi. There"},
	}); err != nil {
		t.Fatal(err)
	}
	var id string
	deadline := time.Now().Add(2 * time.Second)
	for id == "" {
		script.mu.Lock()
		if len(script.sent) > 0 {
			id = script.ids[0]
		}
		script.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no send")
		}
		time.Sleep(time.Millisecond)
	}
	script.push(StreamEvent{
		ContextID: id,
		Audio: AudioChunk{
			PCM:        []byte{1, 2, 3, 4},
			SampleRate: 16000,
			Alignment: &Alignment{
				Chars:   []string{"H", "i", "."},
				StartMS: []int{0, 40, 80},
				DurMS:   []int{40, 40, 40},
			},
		},
	})
	deadline = time.Now().Add(2 * time.Second)
	for !sawTextPTS(rec, "Hi.", 120*time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("frames %s", kindsOf(rec))
		}
		time.Sleep(time.Millisecond)
	}
	if err := task.QueueFrame(context.Background(), &frames.InterruptionFrame{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		script.mu.Lock()
		n := len(script.closedIDs)
		script.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("context was not closed")
		}
		time.Sleep(time.Millisecond)
	}
	before := audioCount(rec)
	script.push(StreamEvent{ContextID: id, Audio: AudioChunk{PCM: []byte{9, 9}, SampleRate: 16000}})
	time.Sleep(40 * time.Millisecond)
	if audioCount(rec) != before {
		t.Fatalf("stale audio delivered: %d -> %d", before, audioCount(rec))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
	}
}

func TestStreamingFinalPairsStopped(t *testing.T) {
	script := newScripted()
	script.auto = true
	tts := NewStreaming(script, Options{Name: "tts", SampleRate: 16000, PushTextFrames: true})
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(tts, rec.Processor("rec")))
	pipelinetest.UntilEnd(t, task, func() {
		if err := task.QueueFrames(context.Background(), []frames.Frame{
			&frames.LLMFullResponseStartFrame{},
			&frames.LLMTextFrame{Text: "Hello there. And more."},
			&frames.LLMFullResponseEndFrame{},
		}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for !saw(rec, frames.KindTTSStopped) {
			if time.Now().After(deadline) {
				t.Fatalf("frames %s", kindsOf(rec))
			}
			time.Sleep(time.Millisecond)
		}
	})
	if !saw(rec, frames.KindTTSStarted) || !saw(rec, frames.KindTTSStopped) {
		t.Fatalf("frames %s", kindsOf(rec))
	}
	if audioCount(rec) < 1 {
		t.Fatal("no audio")
	}
}

type scripted struct {
	mu        sync.Mutex
	ch        chan StreamEvent
	sent      []string
	ids       []string
	closedIDs []string
	auto      bool
	closed    bool
}

func newScripted() *scripted {
	return &scripted{ch: make(chan StreamEvent, 8)}
}

func (s *scripted) Connect(context.Context) error { return nil }

func (s *scripted) SendText(ctx context.Context, id, text string) error {
	s.mu.Lock()
	s.sent = append(s.sent, text)
	s.ids = append(s.ids, id)
	auto := s.auto
	s.mu.Unlock()
	if auto {
		s.push(StreamEvent{
			ContextID: id,
			Audio:     AudioChunk{PCM: []byte{1, 2, 3, 4}, SampleRate: 16000},
		})
	}
	return nil
}

func (s *scripted) Flush(ctx context.Context, id string) error {
	s.push(StreamEvent{ContextID: id, Final: true})
	return nil
}

func (s *scripted) CloseContext(ctx context.Context, id string) error {
	s.mu.Lock()
	s.closedIDs = append(s.closedIDs, id)
	s.mu.Unlock()
	return nil
}

func (s *scripted) Receive() <-chan StreamEvent { return s.ch }

func (s *scripted) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return nil
}

func (s *scripted) push(ev StreamEvent) {
	s.ch <- ev
}

func waitRunning(t *testing.T, task *pipeline.PipelineTask, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		select {
		case err := <-done:
			t.Fatalf("task ended: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
}

func saw(rec *pipelinetest.Recorder, kind string) bool {
	for _, f := range rec.Frames() {
		if f.FrameKind() == kind {
			return true
		}
	}
	return false
}

func sawTextPTS(rec *pipelinetest.Recorder, text string, pts time.Duration) bool {
	for _, f := range rec.Frames() {
		tf, ok := f.(*frames.TTSTextFrame)
		if ok && tf.Text == text && tf.Meta.PTS == pts {
			return true
		}
	}
	return false
}

func audioCount(rec *pipelinetest.Recorder) int {
	n := 0
	for _, f := range rec.Frames() {
		if _, ok := f.(*frames.TTSAudioRawFrame); ok {
			n++
		}
	}
	return n
}

func kindsOf(rec *pipelinetest.Recorder) string {
	var b bytes.Buffer
	for _, f := range rec.Frames() {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(f.FrameKind())
	}
	return b.String()
}
