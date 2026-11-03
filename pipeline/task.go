package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/processor"
)

const (
	defaultDataQueue     = 256
	defaultAudioSysMax   = 500
	defaultCancelTimeout = 20 * time.Second
	defaultSlowWarn      = 20 * time.Millisecond
)

// TaskOptions configures the queued pipeline runtime.
type TaskOptions struct {
	// DataQueueSize is the downstream data lane capacity (default 256).
	DataQueueSize int
	// AudioSysQueueMax drops the oldest input-audio system frame past this
	// depth (default 500).
	AudioSysQueueMax int
	// CancelTimeout is how long Cancel waits for processors to observe
	// CancelFrame before the task context is cancelled (default 20s).
	CancelTimeout time.Duration
	// SlowProcessWarn logs when Process exceeds this (default 20ms).
	SlowProcessWarn time.Duration
	Logger          *slog.Logger
	OnError         func(error)
}

// PipelineTask runs each processor on its own goroutine.
// Downstream data is bounded; system frames skip ahead of data; upstream
// frames are unbounded so neighbouring processors cannot deadlock.
type PipelineTask struct {
	pipeline  *Pipeline
	observers []observe.FrameObserver
	idleObs   *observe.IdleFrameObserver
	opts      TaskOptions
	onError   func(error)
	logger    *slog.Logger

	mu     sync.Mutex
	pre    []staged
	accept bool

	nodes []*node
	real  []*node
	head  *node

	ready       chan struct{}
	doneCh      chan struct{}
	ended       chan struct{}
	cancelReq   chan struct{}
	cancelOnce  sync.Once
	endOnce     sync.Once
	startOnce   sync.Once
	startedSeen chan struct{}
	ackClose    sync.Once
	cancelAcks  chan struct{}
	ackMu       sync.Mutex
	ackNeed     int
	ackGot      int

	rootCancel context.CancelFunc
	runGate    atomic.Bool
	fatalErr   error
	slow       atomic.Int64
	wg         sync.WaitGroup
}

// NewPipelineTask builds a task for the given pipeline. The processor graph
// is fixed at construction; Run starts one goroutine per node.
func NewPipelineTask(p *Pipeline, opts ...TaskOption) *PipelineTask {
	t := &PipelineTask{
		pipeline:    p,
		logger:      slog.Default(),
		ready:       make(chan struct{}),
		doneCh:      make(chan struct{}),
		ended:       make(chan struct{}),
		cancelReq:   make(chan struct{}),
		startedSeen: make(chan struct{}),
		cancelAcks:  make(chan struct{}),
		opts: TaskOptions{
			DataQueueSize:    defaultDataQueue,
			AudioSysQueueMax: defaultAudioSysMax,
			CancelTimeout:    defaultCancelTimeout,
			SlowProcessWarn:  defaultSlowWarn,
		},
	}
	for _, o := range opts {
		o(t)
	}
	if t.opts.DataQueueSize <= 0 {
		t.opts.DataQueueSize = defaultDataQueue
	}
	if t.opts.AudioSysQueueMax <= 0 {
		t.opts.AudioSysQueueMax = defaultAudioSysMax
	}
	if t.opts.CancelTimeout <= 0 {
		t.opts.CancelTimeout = defaultCancelTimeout
	}
	if t.opts.SlowProcessWarn <= 0 {
		t.opts.SlowProcessWarn = defaultSlowWarn
	}
	if t.opts.Logger != nil {
		t.logger = t.opts.Logger
	}
	if t.opts.OnError != nil && t.onError == nil {
		t.onError = t.opts.OnError
	}
	t.build()
	t.ackNeed = len(t.real)
	return t
}

// TaskOption configures PipelineTask.
type TaskOption func(*PipelineTask)

// WithObservers registers frame observers (logging, metrics, idle, etc.).
func WithObservers(obs ...observe.FrameObserver) TaskOption {
	return func(t *PipelineTask) {
		t.observers = append(t.observers, obs...)
	}
}

// WithIdleObserver registers the idle watchdog (also added to observers).
func WithIdleObserver(idle *observe.IdleFrameObserver) TaskOption {
	return func(t *PipelineTask) {
		t.idleObs = idle
		t.observers = append(t.observers, idle)
	}
}

// WithTaskOptions applies queue, timeout, logger, and error settings.
func WithTaskOptions(o TaskOptions) TaskOption {
	return func(t *PipelineTask) {
		if o.DataQueueSize > 0 {
			t.opts.DataQueueSize = o.DataQueueSize
		}
		if o.AudioSysQueueMax > 0 {
			t.opts.AudioSysQueueMax = o.AudioSysQueueMax
		}
		if o.CancelTimeout > 0 {
			t.opts.CancelTimeout = o.CancelTimeout
		}
		if o.SlowProcessWarn > 0 {
			t.opts.SlowProcessWarn = o.SlowProcessWarn
		}
		if o.Logger != nil {
			t.opts.Logger = o.Logger
		}
		if o.OnError != nil {
			t.opts.OnError = o.OnError
			t.onError = o.OnError
		}
	}
}

// WithOnError receives non-fatal processor errors once they reach the source.
func WithOnError(fn func(error)) TaskOption {
	return func(t *PipelineTask) {
		t.onError = fn
		t.opts.OnError = fn
	}
}

// Running reports whether Run has started the node loops and is accepting
// frames directly into inboxes (frames queued earlier are buffered).
func (t *PipelineTask) Running() bool {
	select {
	case <-t.ready:
		return true
	default:
		return false
	}
}

// Run starts the node loops and blocks until a graceful End (nil), Cancel
// (context.Canceled), or the parent context ends.
func (t *PipelineTask) Run(ctx context.Context) error {
	if !t.runGate.CompareAndSwap(false, true) {
		return errors.New("llmpipe: pipeline task already started")
	}
	defer close(t.doneCh)
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-t.cancelReq:
		return context.Canceled
	default:
	}

	ctx, cancel := context.WithCancel(ctx)
	t.mu.Lock()
	t.rootCancel = cancel
	t.mu.Unlock()
	defer cancel()

	if t.idleObs != nil {
		t.idleObs.Start()
		defer t.idleObs.Stop()
	}

	t.bind(ctx)
	if err := t.setupAll(ctx); err != nil {
		t.cleanupAll()
		return err
	}

	t.wg.Add(len(t.nodes))
	for _, n := range t.nodes {
		go t.runNode(n)
	}
	t.flushPrequeue(ctx)
	close(t.ready)

	var err error
	select {
	case <-t.ended:
		err = nil
	case <-t.cancelReq:
		err = t.finishCancel(ctx)
	case <-ctx.Done():
		err = t.finishCancel(ctx)
	}
	t.forceStop()
	t.wg.Wait()
	t.cleanupAll()
	return err
}

// QueueFrame enqueues one frame at the pipeline head.
func (t *PipelineTask) QueueFrame(ctx context.Context, f frames.Frame) error {
	return t.QueueFrames(ctx, []frames.Frame{f})
}

// QueueFrames enqueues frames at the pipeline head. Frames queued before Run
// are buffered and flushed when the node loops start. Downstream data blocks
// when that processor's lane is full; a cancelled ctx drops the rest.
func (t *PipelineTask) QueueFrames(ctx context.Context, fs []frames.Frame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	if !t.accept {
		for _, f := range fs {
			if f == nil {
				continue
			}
			t.pre = append(t.pre, staged{f: f})
		}
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()
	for _, f := range fs {
		if f == nil {
			continue
		}
		if err := t.enqueueStaged(ctx, staged{f: f}); err != nil {
			return err
		}
	}
	return nil
}

// StopWhenDone queues an EndFrame. Run returns nil after End reaches the sink
// and processors have been cleaned up in reverse order.
func (t *PipelineTask) StopWhenDone() {
	_ = t.QueueFrame(context.Background(), &frames.EndFrame{})
}

// Cancel requests a shutdown. Run pushes CancelFrame, waits up to
// CancelTimeout for every processor to observe it, then cancels the task
// context. Extra calls are no-ops.
func (t *PipelineTask) Cancel() {
	t.cancelOnce.Do(func() {
		close(t.cancelReq)
	})
}

// Done is closed when Run returns.
func (t *PipelineTask) Done() <-chan struct{} { return t.doneCh }

// Handle returns the live handle for the first processor with name.
// The handle is valid once the task has been constructed.
func (t *PipelineTask) Handle(name string) (processor.Handle, bool) {
	for _, n := range t.real {
		if n.name == name && n.h != nil {
			return n.h, true
		}
	}
	return nil, false
}

// StartSession queues sf and blocks until that StartFrame reaches the sink.
func (t *PipelineTask) StartSession(ctx context.Context, sf *frames.StartFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sf == nil {
		sf = &frames.StartFrame{}
	}
	if err := t.QueueFrames(ctx, []frames.Frame{sf}); err != nil {
		return err
	}
	select {
	case <-t.startedSeen:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.doneCh:
		return errors.New("llmpipe: task ended before StartFrame reached the sink")
	}
}

// ProcessorIndex returns the index of the first processor with the given name, or -1.
func (t *PipelineTask) ProcessorIndex(name string) int {
	for _, n := range t.real {
		if n.name == name {
			return n.index
		}
	}
	return -1
}

// Reenter continues downstream of the processor at afterIdx.
//
// Deprecated: queue with processor.Handle.PushFrame. Reenter only enqueues;
// it does not call Process on the caller's goroutine. Removed in v0.4.
func (t *PipelineTask) Reenter(ctx context.Context, afterIdx int, f frames.Frame) error {
	if afterIdx < 0 || afterIdx >= len(t.real) {
		return nil
	}
	n := t.real[afterIdx]
	if len(n.down) == 0 || f == nil {
		return nil
	}
	target := n.down[0]
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	if !t.accept {
		t.pre = append(t.pre, staged{f: f, target: target})
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()
	return target.in.push(ctx, queued{frame: f, dir: processor.Downstream})
}

// ReenterAfter resolves a processor name and queues downstream of it.
//
// Deprecated: use processor.Handle from Setup. Removed in v0.4.
func (t *PipelineTask) ReenterAfter(ctx context.Context, processorName string, f frames.Frame) error {
	i := t.ProcessorIndex(processorName)
	if i < 0 {
		return fmt.Errorf("llmpipe: unknown processor %q", processorName)
	}
	return t.Reenter(ctx, i, f)
}
