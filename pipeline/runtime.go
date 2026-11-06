package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/processor"
)

type nodeKind uint8

const (
	kindProc nodeKind = iota
	kindFan
	kindJoin
)

type node struct {
	name     string
	kind     nodeKind
	proc     processor.Processor
	index    int
	in       *inbox
	down     []*node
	up       []*node
	upTarget *node // join: upstream enters this node (branch 0 tail, or the fanout)
	h        *procHandle
	ctx      context.Context
	branch   int
	fanID    uint64
	fanSeq   atomic.Uint64
	join     *joinState
	didSetup bool
	cleaned  bool
	ackOnce  sync.Once
}

type joinState struct {
	mu      sync.Mutex
	n       int
	fanID   uint64
	next    uint64
	epochs  map[uint64]*epochBuf
	foreign map[barrierKey]int
}

type barrierKey struct {
	fan   uint64
	epoch uint64
}

type epochBuf struct {
	per   [][]frames.Frame
	seen  []bool
	dones int
}

type staged struct {
	f      frames.Frame
	target *node // nil queues at the pipeline head
}

type procHandle struct {
	n          *node
	t          *PipelineTask
	logger     *slog.Logger
	mu         sync.Mutex
	procCtx    context.Context
	turn       context.Context
	turnCancel context.CancelFunc
}

var nextFanID atomic.Uint64

func (t *PipelineTask) build() {
	var procs []processor.Processor
	if t.pipeline != nil {
		procs = t.pipeline.Processors()
	}
	nodes, head, _ := buildSegment(procs, t)
	if head == nil {
		head = &node{
			name:   "pipeline",
			kind:   kindProc,
			index:  -1,
			branch: -1,
			in:     newInbox(t.opts.DataQueueSize, t.opts.AudioSysQueueMax),
		}
		nodes = []*node{head}
	}
	t.nodes = nodes
	t.head = head
}

func buildSegment(procs []processor.Processor, t *PipelineTask) (all []*node, head, tail *node) {
	var prev *node
	for _, p := range procs {
		var seg []*node
		var h, tl *node
		if pp, ok := p.(*ParallelPipeline); ok {
			h, tl, seg = buildParallel(pp, t)
		} else {
			n := newProcNode(p, t)
			h, tl, seg = n, n, []*node{n}
		}
		if prev != nil {
			linkDown(prev, h)
		}
		all = append(all, seg...)
		if head == nil {
			head = h
		}
		prev = tl
		tail = tl
	}
	return all, head, tail
}

func buildParallel(pp *ParallelPipeline, t *PipelineTask) (fan, join *node, all []*node) {
	id := nextFanID.Add(1)
	fan = &node{
		name:   pp.name + "/fanout",
		kind:   kindFan,
		index:  -1,
		branch: -1,
		fanID:  id,
		in:     newInbox(t.opts.DataQueueSize, t.opts.AudioSysQueueMax),
	}
	js := &joinState{
		n:      len(pp.branches),
		fanID:  id,
		next:   1,
		epochs: map[uint64]*epochBuf{},
	}
	join = &node{
		name:   pp.name + "/join",
		kind:   kindJoin,
		index:  -1,
		branch: -1,
		fanID:  id,
		join:   js,
		in:     newInbox(t.opts.DataQueueSize, t.opts.AudioSysQueueMax),
	}
	all = append(all, fan)
	var branch0Tail *node
	for i, br := range pp.branches {
		if len(br) == 0 {
			fan.down = append(fan.down, join)
			if i == 0 {
				branch0Tail = nil
			}
			continue
		}
		seg, h, tl := buildSegment(br, t)
		all = append(all, seg...)
		fan.down = append(fan.down, h)
		h.up = append(h.up, fan)
		tl.down = append(tl.down, join)
		tl.branch = i
		if i == 0 {
			branch0Tail = tl
		}
	}
	if branch0Tail != nil {
		join.upTarget = branch0Tail
	} else {
		join.upTarget = fan
	}
	all = append(all, join)
	return fan, join, all
}

func newProcNode(p processor.Processor, t *PipelineTask) *node {
	n := &node{
		name:   p.Name(),
		kind:   kindProc,
		proc:   p,
		index:  len(t.real),
		branch: -1,
		in:     newInbox(t.opts.DataQueueSize, t.opts.AudioSysQueueMax),
	}
	n.h = newProcHandle(n, t)
	t.real = append(t.real, n)
	return n
}

func linkDown(from, to *node) {
	from.down = append(from.down, to)
	to.up = append(to.up, from)
}

func newProcHandle(n *node, t *PipelineTask) *procHandle {
	return &procHandle{n: n, t: t, logger: t.logger.With("processor", n.name)}
}

func (h *procHandle) bind(ctx context.Context) {
	h.mu.Lock()
	h.procCtx = ctx
	if h.turnCancel != nil {
		h.turnCancel()
	}
	h.turn, h.turnCancel = context.WithCancel(ctx)
	h.mu.Unlock()
}

func (h *procHandle) renewTurn() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.turnCancel != nil {
		h.turnCancel()
	}
	parent := h.procCtx
	if parent == nil {
		parent = context.Background()
	}
	h.turn, h.turnCancel = context.WithCancel(parent)
}

func (h *procHandle) cancelTurn() {
	h.mu.Lock()
	if h.turnCancel != nil {
		h.turnCancel()
	}
	h.mu.Unlock()
}

func (h *procHandle) PushFrame(f frames.Frame, dir processor.Direction) error {
	return h.PushFrameCtx(context.Background(), f, dir)
}

func (h *procHandle) PushFrameCtx(ctx context.Context, f frames.Frame, dir processor.Direction) error {
	if f == nil {
		return nil
	}
	if dir == processor.Upstream {
		return h.t.deliverUp(ctx, h.n, f)
	}
	return h.t.deliverDown(ctx, h.n, f, 0, 0, ctrlNone)
}

func (h *procHandle) QueueSelf(f frames.Frame) error {
	if f == nil {
		return nil
	}
	return h.n.in.push(context.Background(), queued{frame: f, dir: processor.Downstream})
}

func (h *procHandle) TurnContext() context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.turn == nil {
		return context.Background()
	}
	return h.turn
}

func (h *procHandle) Context() context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.procCtx == nil {
		return context.Background()
	}
	return h.procCtx
}

func (h *procHandle) Name() string { return h.n.name }

func (h *procHandle) Logger() *slog.Logger {
	if h.logger == nil {
		return slog.Default()
	}
	return h.logger
}

func (t *PipelineTask) bind(ctx context.Context) {
	for _, n := range t.nodes {
		n.ctx = ctx
		if n.h != nil {
			n.h.bind(ctx)
		}
	}
}

func (t *PipelineTask) setupAll(ctx context.Context) error {
	for _, n := range t.real {
		s, ok := n.proc.(processor.Setupper)
		if !ok {
			continue
		}
		if err := s.Setup(ctx, n.h); err != nil {
			return err
		}
		n.didSetup = true
	}
	return nil
}

func (t *PipelineTask) cleanupAll() {
	ctx := context.Background()
	for i := len(t.real) - 1; i >= 0; i-- {
		n := t.real[i]
		if n.cleaned {
			continue
		}
		c, ok := n.proc.(processor.Cleanuper)
		if !ok {
			n.cleaned = true
			continue
		}
		// Skip processors whose Setup failed before it ran. Processors with
		// only Cleanup still run.
		if _, isSetup := n.proc.(processor.Setupper); isSetup && !n.didSetup {
			n.cleaned = true
			continue
		}
		n.cleaned = true
		_ = c.Cleanup(ctx)
	}
}

func (t *PipelineTask) flushPrequeue(ctx context.Context) {
	for {
		t.mu.Lock()
		pre := t.pre
		t.pre = nil
		if len(pre) == 0 {
			t.accept = true
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()
		for _, s := range pre {
			_ = t.enqueueStaged(ctx, s)
		}
	}
}

func (t *PipelineTask) enqueueStaged(ctx context.Context, s staged) error {
	n := s.target
	if n == nil {
		n = t.head
	}
	if n == nil || s.f == nil {
		return nil
	}
	return n.in.push(ctx, queued{frame: s.f, dir: processor.Downstream})
}

func (t *PipelineTask) runNode(n *node) {
	defer t.wg.Done()
	ctx := n.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		q, ok := n.in.pop(ctx)
		if !ok {
			return
		}
		switch n.kind {
		case kindFan:
			t.onFan(n, q)
		case kindJoin:
			t.onJoin(n, q)
		default:
			t.onProc(n, q)
		}
	}
}

func (t *PipelineTask) onFan(n *node, q queued) {
	ctx := n.ctx
	if q.dir == processor.Upstream {
		_ = t.deliverUp(ctx, n, q.frame)
		return
	}
	if q.ctrl == ctrlBarrier {
		for i, d := range n.down {
			qq := q
			qq.branch = i
			_ = d.in.push(ctx, qq)
		}
		return
	}
	epoch := n.fanSeq.Add(1)
	q.epoch = epoch
	q.fanID = n.fanID
	q.dir = processor.Downstream
	for i, d := range n.down {
		qq := q
		qq.branch = i
		_ = d.in.push(ctx, qq)
	}
	done := queued{ctrl: ctrlBarrier, epoch: epoch, fanID: n.fanID, dir: processor.Downstream}
	for i, d := range n.down {
		qq := done
		qq.branch = i
		_ = d.in.push(ctx, qq)
	}
}

func (t *PipelineTask) onJoin(n *node, q queued) {
	if q.dir == processor.Upstream {
		if n.upTarget != nil {
			_ = n.upTarget.in.push(n.ctx, queued{frame: q.frame, dir: processor.Upstream})
			return
		}
		t.onSourceUpstream(q.frame)
		return
	}
	js := n.join
	if js == nil {
		return
	}
	if q.ctrl == ctrlBarrier && q.fanID != js.fanID {
		t.forwardForeign(n, q)
		return
	}
	var ready [][]frames.Frame
	js.mu.Lock()
	if q.epoch == 0 && q.ctrl == ctrlNone {
		js.mu.Unlock()
		_ = t.deliverDown(n.ctx, n, q.frame, 0, 0, ctrlNone)
		return
	}
	b := js.buf(q.epoch)
	if q.ctrl == ctrlBarrier {
		if q.branch >= 0 && q.branch < len(b.seen) && !b.seen[q.branch] {
			b.seen[q.branch] = true
			b.dones++
		}
	} else if q.frame != nil && q.branch >= 0 && q.branch < len(b.per) {
		b.per[q.branch] = append(b.per[q.branch], q.frame)
	}
	for {
		cur := js.epochs[js.next]
		if cur == nil || cur.dones < js.n {
			break
		}
		ready = append(ready, mergeEpoch(cur.per))
		delete(js.epochs, js.next)
		js.next++
	}
	js.mu.Unlock()
	for _, batch := range ready {
		for _, f := range batch {
			_ = t.deliverDown(n.ctx, n, f, 0, 0, ctrlNone)
		}
	}
}

func (t *PipelineTask) forwardForeign(n *node, q queued) {
	js := n.join
	js.mu.Lock()
	if js.foreign == nil {
		js.foreign = map[barrierKey]int{}
	}
	k := barrierKey{fan: q.fanID, epoch: q.epoch}
	js.foreign[k]++
	full := js.foreign[k] >= js.n
	if full {
		delete(js.foreign, k)
	}
	js.mu.Unlock()
	if full {
		_ = t.deliverDown(n.ctx, n, nil, q.epoch, q.fanID, ctrlBarrier)
	}
}

func (j *joinState) buf(epoch uint64) *epochBuf {
	b := j.epochs[epoch]
	if b == nil {
		b = &epochBuf{
			per:  make([][]frames.Frame, j.n),
			seen: make([]bool, j.n),
		}
		j.epochs[epoch] = b
	}
	return b
}

func (t *PipelineTask) onProc(n *node, q queued) {
	if q.ctrl == ctrlBarrier {
		_ = t.deliverDown(n.ctx, n, nil, q.epoch, q.fanID, ctrlBarrier)
		return
	}
	if n.proc == nil {
		if q.dir == processor.Upstream {
			_ = t.deliverUp(n.ctx, n, q.frame)
		} else {
			_ = t.deliverDown(n.ctx, n, q.frame, q.epoch, q.fanID, ctrlNone)
		}
		return
	}
	switch q.frame.(type) {
	case *frames.InterruptionFrame:
		n.in.drainInterruptible()
		if n.h != nil {
			n.h.renewTurn()
		}
	case *frames.CancelFrame:
		if n.h != nil {
			n.h.cancelTurn()
		}
		t.noteCancel(n)
	}
	t.notify(n, q.frame, q.dir)

	ctx := n.ctx
	if n.h != nil && n.h.Context() != nil {
		ctx = n.h.Context()
	}
	var forwardedDown, forwardedUp bool
	emit := processor.Emit{
		Down: func(ff frames.Frame) {
			if ff == nil {
				return
			}
			if ff == q.frame {
				forwardedDown = true
			}
			_ = t.deliverDown(n.ctx, n, ff, q.epoch, q.fanID, ctrlNone)
		},
		Up: func(ff frames.Frame) {
			if ff == nil {
				return
			}
			if ff == q.frame {
				forwardedUp = true
			}
			_ = t.deliverUp(n.ctx, n, ff)
		},
	}
	start := time.Now()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("llmpipe: panic in %s: %v\n%s", n.name, r, debug.Stack())
				fe := &frames.FatalErrorFrame{Err: err}
				_ = t.deliverUp(n.ctx, n, fe)
				err = nil
				t.Cancel()
			}
		}()
		err = n.proc.Process(ctx, q.frame, q.dir, emit)
	}()
	if warn := t.opts.SlowProcessWarn; warn > 0 {
		if d := time.Since(start); d > warn {
			t.slow.Add(1)
			t.logger.Warn("slow processor", "processor", n.name, "elapsed", d.String())
		}
	}
	if err != nil {
		_ = t.deliverUp(n.ctx, n, &frames.ErrorFrame{Err: err})
	}
	if q.dir == processor.Downstream && !forwardedDown && autoForwardDown(q.frame) {
		_ = t.deliverDown(n.ctx, n, q.frame, q.epoch, q.fanID, ctrlNone)
	}
	if q.dir == processor.Upstream && !forwardedUp && autoForwardUp(q.frame) {
		_ = t.deliverUp(n.ctx, n, q.frame)
	}
}

func (t *PipelineTask) notify(n *node, f frames.Frame, dir processor.Direction) {
	if n == nil || n.proc == nil || len(t.observers) == 0 || f == nil {
		return
	}
	ev := observe.FramePushed{Frame: f, Direction: dir, Processor: n.name, Index: n.index}
	for _, o := range t.observers {
		o.OnFrame(ev)
	}
}

func (t *PipelineTask) deliverDown(ctx context.Context, from *node, f frames.Frame, epoch, fanID uint64, ctrl ctrlKind) error {
	q := queued{frame: f, dir: processor.Downstream, epoch: epoch, fanID: fanID, ctrl: ctrl}
	if len(from.down) == 0 {
		if ctrl == ctrlNone && f != nil {
			t.onSink(f)
		}
		return nil
	}
	var err error
	for _, d := range from.down {
		qq := q
		if from.branch >= 0 && d.kind == kindJoin {
			qq.branch = from.branch
		}
		if e := d.in.push(ctx, qq); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (t *PipelineTask) deliverUp(ctx context.Context, from *node, f frames.Frame) error {
	if f == nil {
		return nil
	}
	if len(from.up) == 0 {
		t.onSourceUpstream(f)
		return nil
	}
	q := queued{frame: f, dir: processor.Upstream}
	var err error
	for _, u := range from.up {
		if e := u.in.push(ctx, q); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (t *PipelineTask) onSink(f frames.Frame) {
	switch f.(type) {
	case *frames.StartFrame:
		t.startOnce.Do(func() { close(t.startedSeen) })
	case *frames.EndFrame, *frames.StopFrame:
		t.endOnce.Do(func() { close(t.ended) })
	}
}

func (t *PipelineTask) onSourceUpstream(f frames.Frame) {
	switch fr := f.(type) {
	case *frames.InterruptionTaskFrame:
		if t.head != nil {
			_ = t.head.in.push(context.Background(), queued{
				frame: &frames.InterruptionFrame{},
				dir:   processor.Downstream,
			})
		}
	case *frames.FatalErrorFrame:
		t.mu.Lock()
		if t.fatalErr == nil {
			t.fatalErr = fr.Err
		}
		t.mu.Unlock()
		t.Cancel()
	case *frames.ErrorFrame:
		if t.onError != nil && fr.Err != nil {
			t.onError(fr.Err)
		}
	}
}

func (t *PipelineTask) noteCancel(n *node) {
	n.ackOnce.Do(func() {
		t.ackMu.Lock()
		t.ackGot++
		done := t.ackNeed > 0 && t.ackGot >= t.ackNeed
		t.ackMu.Unlock()
		if done {
			t.ackClose.Do(func() { close(t.cancelAcks) })
		}
	})
}

func (t *PipelineTask) finishCancel(parent context.Context) error {
	if parent.Err() != nil {
		t.forceStop()
		return parent.Err()
	}
	for _, n := range t.real {
		_ = n.in.push(context.Background(), queued{frame: &frames.CancelFrame{}, dir: processor.Downstream})
	}
	if len(t.real) == 0 {
		t.ackClose.Do(func() { close(t.cancelAcks) })
	}
	timer := time.NewTimer(t.opts.CancelTimeout)
	defer timer.Stop()
	select {
	case <-t.cancelAcks:
	case <-t.ended:
	case <-parent.Done():
	case <-timer.C:
		if t.rootCancel != nil {
			t.rootCancel()
		}
	}
	t.forceStop()
	return context.Canceled
}

func (t *PipelineTask) forceStop() {
	if t.rootCancel != nil {
		t.rootCancel()
	}
	for _, n := range t.nodes {
		if n.in != nil {
			n.in.close()
		}
	}
}

func autoForwardDown(f frames.Frame) bool {
	switch f.(type) {
	case *frames.StartFrame, *frames.EndFrame, *frames.CancelFrame:
		return true
	default:
		return false
	}
}

func autoForwardUp(f frames.Frame) bool {
	switch f.(type) {
	case *frames.ErrorFrame, *frames.FatalErrorFrame:
		return true
	default:
		return false
	}
}

func isSyncFrame(f frames.Frame) bool {
	switch f.(type) {
	case *frames.StartFrame, *frames.EndFrame, *frames.CancelFrame:
		return true
	default:
		return false
	}
}

func mergeEpoch(per [][]frames.Frame) []frames.Frame {
	var syncf frames.Frame
	var rest []frames.Frame
	for _, branch := range per {
		for _, f := range branch {
			if f == nil {
				continue
			}
			if syncf == nil && isSyncFrame(f) {
				syncf = f
				continue
			}
			if syncf != nil && f == syncf {
				continue
			}
			rest = append(rest, f)
		}
	}
	rest = dedupeFrames(rest)
	if syncf == nil {
		return rest
	}
	switch syncf.(type) {
	case *frames.EndFrame, *frames.CancelFrame:
		return append(rest, syncf)
	default:
		out := make([]frames.Frame, 0, len(rest)+1)
		out = append(out, syncf)
		return append(out, rest...)
	}
}

func dedupeFrames(in []frames.Frame) []frames.Frame {
	if len(in) < 2 {
		return in
	}
	seen := make(map[uint64]struct{}, len(in))
	out := make([]frames.Frame, 0, len(in))
	for _, f := range in {
		id := frames.MetaID(f)
		if id != 0 {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
		}
		out = append(out, f)
	}
	return out
}
