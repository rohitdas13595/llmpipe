package processor

import (
	"context"
	"log/slog"

	"github.com/rohitdas13595/llmpipe/frames"
)

// Handle is the processor-owned gate into the pipeline. Process runs on the
// processor goroutine only; any other goroutine must use Handle to enqueue
// frames. PushFrame blocks when the downstream data lane is full.
// PushFrameCtx returns when ctx is cancelled instead of waiting.
type Handle interface {
	PushFrame(f frames.Frame, dir Direction) error
	PushFrameCtx(ctx context.Context, f frames.Frame, dir Direction) error
	// QueueSelf enqueues f onto this processor so Process runs on its goroutine.
	QueueSelf(f frames.Frame) error
	// TurnContext is cancelled on InterruptionFrame and Cancel, then replaced.
	TurnContext() context.Context
	// Context is the processor context. It outlives a single turn and is
	// cancelled when the task is force-stopped.
	Context() context.Context
	Name() string
	Logger() *slog.Logger
}
