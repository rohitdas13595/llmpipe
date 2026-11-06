package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

type lifeProc struct {
	name     string
	order    *[]string
	mu       *sync.Mutex
	block    chan struct{}
	entered  chan struct{}
	panicOn  bool
	sawTurn1 context.Context
	sawTurn2 context.Context
	h        processor.Handle
}

func (p *lifeProc) Name() string { return p.name }

func (p *lifeProc) Setup(ctx context.Context, h processor.Handle) error {
	p.h = h
	p.mu.Lock()
	*p.order = append(*p.order, "setup "+p.name)
	p.mu.Unlock()
	return nil
}

func (p *lifeProc) Cleanup(ctx context.Context) error {
	p.mu.Lock()
	*p.order = append(*p.order, "cleanup "+p.name)
	p.mu.Unlock()
	return nil
}

func (p *lifeProc) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if p.panicOn {
		if _, ok := f.(*frames.TextFrame); ok {
			panic("boom")
		}
	}
	if p.block != nil {
		if _, ok := f.(*frames.TextFrame); ok {
			if p.entered != nil {
				select {
				case <-p.entered:
				default:
					close(p.entered)
				}
			}
			select {
			case <-p.block:
			case <-ctx.Done():
			}
		}
	}
	if _, ok := f.(*frames.InterruptionFrame); ok && p.h != nil {
		p.sawTurn2 = p.h.TurnContext()
	}
	if tf, ok := f.(*frames.TextFrame); ok && tf.Text == "first" && p.h != nil {
		p.sawTurn1 = p.h.TurnContext()
	}
	emit.Down(f)
	return nil
}

func TestEndDrainsQueuedData(t *testing.T) {
	var got []string
	p := processor.Func{
		N: "sink",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				got = append(got, tf.Text)
			}
			emit.Down(f)
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p))
	var framesIn []frames.Frame
	for i := 0; i < 100; i++ {
		framesIn = append(framesIn, &frames.TextFrame{Text: "x"})
	}
	if err := task.QueueFrames(context.Background(), framesIn); err != nil {
		t.Fatal(err)
	}
	task.StopWhenDone()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("drained %d", len(got))
	}
}

func TestCancelTimeoutUnblocks(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	p := processor.Func{
		N: "block",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p), WithTaskOptions(TaskOptions{CancelTimeout: 150 * time.Millisecond}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "hold"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("processor did not start")
	}
	start := time.Now()
	task.Cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not finish")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancel took %s", time.Since(start))
	}
	close(release)
}

func TestCleanupOnceReversed(t *testing.T) {
	var order []string
	var mu sync.Mutex
	mk := func(name string) *lifeProc {
		return &lifeProc{name: name, order: &order, mu: &mu}
	}
	task := NewPipelineTask(NewPipeline(mk("a"), mk("b"), mk("c")))
	task.StopWhenDone()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"setup a", "setup b", "setup c", "cleanup c", "cleanup b", "cleanup a"}
	if len(order) != len(want) {
		t.Fatalf("order %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order %v", order)
		}
	}
}

func TestPanicBecomesFatalError(t *testing.T) {
	var mu sync.Mutex
	var order []string
	bad := &lifeProc{name: "bad", order: &order, mu: &mu, panicOn: true}
	task := NewPipelineTask(NewPipeline(bad))
	if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	err := task.Run(context.Background())
	if err != context.Canceled {
		t.Fatalf("run err %v", err)
	}
	if task.fatalErr == nil {
		t.Fatal("missing fatal")
	}
}

func TestGoleakAfterEnd(t *testing.T) {
	defer goleak.VerifyNone(t)
	task := NewPipelineTask(NewPipeline(processor.Func{
		N: "id",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			emit.Down(f)
			return nil
		},
	}))
	_ = task.QueueFrame(context.Background(), &frames.TextFrame{Text: "x"})
	task.StopWhenDone()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptionDrainsAndRenewsTurn(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var turn1 context.Context
	var turn2 context.Context
	var mu sync.Mutex
	var texts []string
	p := &turnProc{
		entered: entered,
		release: release,
		once:    &once,
		mu:      &mu,
		texts:   &texts,
		turn1:   &turn1,
		turn2:   &turn2,
	}
	task := NewPipelineTask(NewPipeline(p))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("not entered")
	}
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TextFrame{Text: "drop-me"},
		&frames.FunctionCallResultFrame{Result: "keep"},
		&frames.InterruptionFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ready := turn2 != nil
		mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("interruption was not processed")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	got := append([]string(nil), texts...)
	t1, t2 := turn1, turn2
	mu.Unlock()
	if len(got) != 1 || got[0] != "first" {
		t.Fatalf("texts %v", got)
	}
	if t1 == nil || t1.Err() == nil {
		t.Fatal("old turn was not cancelled")
	}
	if t2 == nil || t2.Err() != nil {
		t.Fatal("turn was not renewed")
	}
	if t1 == t2 {
		t.Fatal("turn context was not replaced")
	}
	cancel()
	<-done
}

type turnProc struct {
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
	mu      *sync.Mutex
	texts   *[]string
	turn1   *context.Context
	turn2   *context.Context
	h       processor.Handle
}

func (p *turnProc) Name() string { return "turn" }
func (p *turnProc) Setup(ctx context.Context, h processor.Handle) error {
	p.h = h
	return nil
}
func (p *turnProc) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.TextFrame:
		if fr.Text == "first" {
			p.once.Do(func() {
				*p.turn1 = p.h.TurnContext()
				close(p.entered)
			})
			<-p.release
		}
		p.mu.Lock()
		*p.texts = append(*p.texts, fr.Text)
		p.mu.Unlock()
	case *frames.InterruptionFrame:
		p.mu.Lock()
		*p.turn2 = p.h.TurnContext()
		p.mu.Unlock()
	}
	emit.Down(f)
	return nil
}

func TestTenThousandFramesInOrder(t *testing.T) {
	const n = 10000
	var got []int
	p := processor.Func{
		N: "n",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				var v int
				for _, c := range tf.Text {
					v = v*10 + int(c-'0')
				}
				got = append(got, v)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p))
	batch := make([]frames.Frame, n)
	for i := 0; i < n; i++ {
		batch[i] = &frames.TextFrame{Text: itoa(i)}
	}
	if err := task.QueueFrames(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	task.StopWhenDone()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("len %d", len(got))
	}
	for i := 0; i < n; i++ {
		if got[i] != i {
			t.Fatalf("at %d got %d", i, got[i])
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestBackpressureHiWater(t *testing.T) {
	const capN = 4
	p := processor.Func{
		N: "slow",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.TextFrame); ok {
				time.Sleep(time.Millisecond)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p), WithTaskOptions(TaskOptions{DataQueueSize: capN}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	for i := 0; i < 40; i++ {
		if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "q"}); err != nil {
			t.Fatal(err)
		}
	}
	task.StopWhenDone()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	hi, _ := task.head.in.stats()
	if hi > capN {
		t.Fatalf("hi-water %d > %d", hi, capN)
	}
}

func TestPingPongNoDeadlock(t *testing.T) {
	const hops = 200
	var n atomic.Int32
	a := processor.Func{
		N: "a",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				emit.Down(f)
				return nil
			}
			if dir == processor.Upstream {
				if n.Add(1) >= hops {
					return nil
				}
				emit.Down(f)
				return nil
			}
			emit.Down(f)
			return nil
		},
	}
	b := processor.Func{
		N: "b",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if _, ok := f.(*frames.EndFrame); ok {
				return nil
			}
			if dir == processor.Downstream {
				emit.Up(f)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(a, b))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	waitRunning(t, task, done)
	if err := task.QueueFrame(context.Background(), &frames.TextFrame{Text: "ping"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for n.Load() < hops {
		select {
		case <-deadline:
			t.Fatalf("ping-pong stuck at %d", n.Load())
		case <-time.After(time.Millisecond):
		}
	}
	task.StopWhenDone()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not end")
	}
}

func TestFramesQueuedBeforeRunStayOrdered(t *testing.T) {
	var got []string
	p := processor.Func{
		N: "p",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			if tf, ok := f.(*frames.TextFrame); ok {
				got = append(got, tf.Text)
			}
			return nil
		},
	}
	task := NewPipelineTask(NewPipeline(p))
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.TextFrame{Text: "a"},
		&frames.TextFrame{Text: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	task.StopWhenDone()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}
