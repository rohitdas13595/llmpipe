package pipeline

import (
	"context"
	"errors"
	"sync"

	"github.com/gammazero/deque"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// ErrClosed is returned when a frame is pushed after the task has stopped.
var ErrClosed = errors.New("llmpipe: task closed")

type ctrlKind uint8

const (
	ctrlNone ctrlKind = iota
	ctrlBarrier
)

// queued is one inbox item. Barrier items are internal to parallel join
// and are never delivered to Process.
type queued struct {
	frame  frames.Frame
	dir    processor.Direction
	ctrl   ctrlKind
	epoch  uint64
	fanID  uint64
	branch int
}

// inbox is a two-lane queue: system frames (unbounded, except input audio)
// are popped before upstream frames (unbounded) and downstream data
// (bounded, so a slow consumer applies backpressure). Upstream is unbounded
// so a pair of neighbours cannot deadlock by blocking on each other.
type inbox struct {
	mu        sync.Mutex
	cond      *sync.Cond
	sys       deque.Deque[queued]
	down      deque.Deque[queued]
	up        deque.Deque[queued]
	dataMax   int
	audioMax  int
	audioN    int
	closed    bool
	hiWater   int
	audioDrop int
}

func newInbox(dataMax, audioMax int) *inbox {
	if dataMax <= 0 {
		dataMax = defaultDataQueue
	}
	if audioMax <= 0 {
		audioMax = defaultAudioSysMax
	}
	in := &inbox{dataMax: dataMax, audioMax: audioMax}
	in.cond = sync.NewCond(&in.mu)
	return in
}

func (in *inbox) push(ctx context.Context, q queued) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if q.ctrl == ctrlNone && q.frame != nil {
		frames.EnsureMeta(q.frame)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	for in.blocksLocked(q) && !in.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		in.waitLocked(ctx)
	}
	if in.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch in.laneLocked(q) {
	case laneSys:
		if _, ok := q.frame.(*frames.InputAudioRawFrame); ok {
			for in.audioN >= in.audioMax {
				if !in.dropOldestAudioLocked() {
					break
				}
				in.audioDrop++
			}
			in.audioN++
		}
		in.sys.PushBack(q)
	case laneUp:
		in.up.PushBack(q)
	default:
		in.down.PushBack(q)
		if n := in.down.Len(); n > in.hiWater {
			in.hiWater = n
		}
	}
	in.cond.Broadcast()
	return nil
}

type lane int

const (
	laneSys lane = iota
	laneUp
	laneDown
)

func (in *inbox) laneLocked(q queued) lane {
	if q.ctrl == ctrlNone && q.frame != nil && frames.IsSystem(q.frame) {
		return laneSys
	}
	if q.dir == processor.Upstream {
		return laneUp
	}
	return laneDown
}

func (in *inbox) blocksLocked(q queued) bool {
	return in.laneLocked(q) == laneDown && in.down.Len() >= in.dataMax
}

func (in *inbox) dropOldestAudioLocked() bool {
	for i := 0; i < in.sys.Len(); i++ {
		if _, ok := in.sys.At(i).frame.(*frames.InputAudioRawFrame); ok {
			in.sys.Remove(i)
			in.audioN--
			return true
		}
	}
	return false
}

// pop waits for the next item. System frames win, then upstream, then
// downstream data. ok is false when the inbox is closed or ctx is done.
func (in *inbox) pop(ctx context.Context) (queued, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	for in.sys.Len()+in.up.Len()+in.down.Len() == 0 {
		if in.closed || ctx.Err() != nil {
			return queued{}, false
		}
		in.waitLocked(ctx)
	}
	var q queued
	switch {
	case in.sys.Len() > 0:
		q = in.sys.PopFront()
		if _, ok := q.frame.(*frames.InputAudioRawFrame); ok && in.audioN > 0 {
			in.audioN--
		}
	case in.up.Len() > 0:
		q = in.up.PopFront()
	default:
		q = in.down.PopFront()
		in.cond.Broadcast()
	}
	return q, true
}

// drainInterruptible drops interruptible frames from the upstream and
// downstream lanes. System frames and uninterruptible frames (End, function
// results) stay. Internal barrier markers stay.
func (in *inbox) drainInterruptible() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	n := filterInterruptible(&in.down) + filterInterruptible(&in.up)
	if n > 0 {
		in.cond.Broadcast()
	}
	return n
}

func filterInterruptible(q *deque.Deque[queued]) int {
	n := q.Len()
	if n == 0 {
		return 0
	}
	keep := make([]queued, 0, n)
	dropped := 0
	for i := 0; i < n; i++ {
		item := q.PopFront()
		if item.ctrl != ctrlNone || !frames.Interruptible(item.frame) {
			keep = append(keep, item)
			continue
		}
		dropped++
	}
	for _, item := range keep {
		q.PushBack(item)
	}
	return dropped
}

func (in *inbox) close() {
	in.mu.Lock()
	in.closed = true
	in.cond.Broadcast()
	in.mu.Unlock()
}

func (in *inbox) stats() (hiWater, audioDrops int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.hiWater, in.audioDrop
}

// waitLocked waits for a broadcast. Caller holds in.mu. ctx cancellation
// broadcasts without dropping the wakeup: the watcher blocks on the mutex
// until cond.Wait releases it.
func (in *inbox) waitLocked(ctx context.Context) {
	if ctx == nil || ctx.Done() == nil {
		in.cond.Wait()
		return
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			in.mu.Lock()
			in.cond.Broadcast()
			in.mu.Unlock()
		case <-stop:
		}
	}()
	in.cond.Wait()
	close(stop)
}
