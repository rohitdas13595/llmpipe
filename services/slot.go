package services

import (
	"context"
	"fmt"
	"sync"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// Slot stores the pipeline Handle passed to Setup. Embed it so a service
// implements processor.Setupper and can push frames without re-entering Process.
type Slot struct {
	gate sync.RWMutex
	h    processor.Handle
}

// Setup records h. Later calls replace it.
func (s *Slot) Setup(ctx context.Context, h processor.Handle) error {
	s.gate.Lock()
	s.h = h
	s.gate.Unlock()
	return nil
}

// H returns the handle, or nil when the processor is used outside a task.
func (s *Slot) H() processor.Handle {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.h
}

// ProcContext is the processor context (speech work that should survive a
// barge-in). It is context.Background when no handle is set.
func (s *Slot) ProcContext() context.Context {
	if h := s.H(); h != nil {
		return h.Context()
	}
	return context.Background()
}

// TurnContext is cancelled on interruption. It is context.Background when no
// handle is set.
func (s *Slot) TurnContext() context.Context {
	if h := s.H(); h != nil {
		return h.TurnContext()
	}
	return context.Background()
}

// PushUp enqueues f upstream. ReenterFunc only injects downstream, so a
// missing handle is an error rather than a silent wrong-direction send.
func PushUp(ctx context.Context, h processor.Handle, name string, f frames.Frame) error {
	if h == nil {
		return fmt.Errorf("%s: pipeline handle not configured", name)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return h.PushFrameCtx(ctx, f, processor.Upstream)
}

// PushDown enqueues f downstream. A live Handle wins over the deprecated
// ReenterFunc so Process is not called on the caller's goroutine.
func PushDown(ctx context.Context, h processor.Handle, re ReenterFunc, name string, f frames.Frame) error {
	if h != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		return h.PushFrameCtx(ctx, f, processor.Downstream)
	}
	if re != nil {
		return re(ctx, name, f)
	}
	return fmt.Errorf("%s: pipeline handle not configured", name)
}
