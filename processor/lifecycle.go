package processor

import "context"

// Setupper is called once, in pipeline order, before the node loop starts.
type Setupper interface {
	Setup(ctx context.Context, h Handle) error
}

// Cleanuper is called once, in reverse pipeline order, after a graceful End
// or a Cancel has stopped the node loops.
type Cleanuper interface {
	Cleanup(ctx context.Context) error
}
