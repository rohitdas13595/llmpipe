package pipeline

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Runner runs PipelineTask instances until context cancel.
type Runner struct {
	HandleSigint bool
}

// NewRunner creates a runner with optional SIGINT handling.
func NewRunner(handleSigint bool) *Runner {
	return &Runner{HandleSigint: handleSigint}
}

// Run blocks until ctx is done or the task is cancelled. If HandleSigint, SIGINT cancels ctx.
func (r *Runner) Run(ctx context.Context, task *PipelineTask) error {
	if r.HandleSigint {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sig)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-sig:
				task.Cancel()
				cancel()
			case <-ctx.Done():
			}
		}()
		return task.Run(ctx)
	}
	return task.Run(ctx)
}
