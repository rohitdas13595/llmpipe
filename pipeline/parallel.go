package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// ParallelPipeline runs multiple linear processor branches concurrently. Each
// incoming downstream frame is delivered to every branch; outputs are merged
// with pointer-identity deduplication (same as two Identity filters passing one
// TextFrame). StartFrame, EndFrame, and CancelFrame are synchronized: all
// branches finish processing the lifecycle frame before it is emitted once
// downstream, and frames emitted by processors during that window are ordered
// like Pipecat (StartFrame then buffered; buffered then End/Cancel).
type ParallelPipeline struct {
	name     string
	branches [][]processor.Processor
}

// NewParallelPipeline builds a compound processor from one or more branches.
// Each branch is a slice of processors run in order; branches run in parallel.
func NewParallelPipeline(name string, branches ...[]processor.Processor) (*ParallelPipeline, error) {
	if len(branches) == 0 {
		return nil, fmt.Errorf("llmpipe: ParallelPipeline %q needs at least one branch", name)
	}
	for i, b := range branches {
		if b == nil {
			return nil, fmt.Errorf("llmpipe: ParallelPipeline %q branch %d is nil", name, i)
		}
	}
	return &ParallelPipeline{name: name, branches: branches}, nil
}

// MustParallelPipeline is like NewParallelPipeline but panics on error.
func MustParallelPipeline(name string, branches ...[]processor.Processor) *ParallelPipeline {
	pp, err := NewParallelPipeline(name, branches...)
	if err != nil {
		panic(err)
	}
	return pp
}

func (pp *ParallelPipeline) Name() string { return pp.name }

func (pp *ParallelPipeline) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch dir {
	case processor.Upstream:
		if len(pp.branches) == 0 {
			emit.Up(f)
			return nil
		}
		return pp.runBranch(ctx, 0, f, processor.Upstream, func(ff frames.Frame, direction processor.Direction) {
			if direction == processor.Upstream {
				emit.Up(ff)
			}
		})
	case processor.Downstream:
		if isSyncFrame(f) {
			return pp.processSyncDownstream(ctx, f, emit)
		}
		return pp.processDataDownstream(ctx, f, emit)
	default:
		return nil
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

func (pp *ParallelPipeline) processSyncDownstream(ctx context.Context, f frames.Frame, emit processor.Emit) error {
	if len(pp.branches) == 0 {
		emit.Down(f)
		return nil
	}
	var buf []frames.Frame
	var bufMu sync.Mutex
	appendBuf := func(ff frames.Frame) {
		if ff == f {
			return
		}
		bufMu.Lock()
		buf = append(buf, ff)
		bufMu.Unlock()
	}
	var wg sync.WaitGroup
	errs := make([]error, len(pp.branches))
	for i := range pp.branches {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pp.runBranch(ctx, i, f, processor.Downstream, func(ff frames.Frame, direction processor.Direction) {
				if direction == processor.Downstream {
					appendBuf(ff)
				}
			})
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	switch f.(type) {
	case *frames.StartFrame:
		emit.Down(f)
		for _, ff := range buf {
			emit.Down(ff)
		}
	case *frames.EndFrame, *frames.CancelFrame:
		for _, ff := range buf {
			emit.Down(ff)
		}
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (pp *ParallelPipeline) processDataDownstream(ctx context.Context, f frames.Frame, emit processor.Emit) error {
	if len(pp.branches) == 0 {
		emit.Down(f)
		return nil
	}
	outs := make([][]frames.Frame, len(pp.branches))
	errs := make([]error, len(pp.branches))
	var wg sync.WaitGroup
	for i := range pp.branches {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []frames.Frame
			err := pp.runBranch(ctx, i, f, processor.Downstream, func(ff frames.Frame, direction processor.Direction) {
				if direction == processor.Downstream {
					local = append(local, ff)
				}
			})
			outs[i] = local
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	seen := make(map[frames.Frame]struct{})
	for bi := 0; bi < len(outs); bi++ {
		for _, ff := range outs[bi] {
			if _, ok := seen[ff]; ok {
				continue
			}
			seen[ff] = struct{}{}
			emit.Down(ff)
		}
	}
	return nil
}

// runBranch executes one branch like PipelineTask.processAt, invoking sink when
// a frame leaves the end of the branch (downstream) or the top (upstream).
func (pp *ParallelPipeline) runBranch(ctx context.Context, branchIdx int, f frames.Frame, dir processor.Direction, sink func(frames.Frame, processor.Direction)) error {
	procs := pp.branches[branchIdx]
	if len(procs) == 0 {
		if dir == processor.Downstream {
			sink(f, processor.Downstream)
		} else {
			sink(f, processor.Upstream)
		}
		return nil
	}
	startIdx := 0
	if dir == processor.Upstream {
		startIdx = len(procs) - 1
	}
	var processAt func(idx int, f frames.Frame, dir processor.Direction) error
	processAt = func(idx int, f frames.Frame, dir processor.Direction) error {
		if idx < 0 || idx >= len(procs) {
			if dir == processor.Downstream && idx >= len(procs) {
				sink(f, processor.Downstream)
			}
			if dir == processor.Upstream && idx < 0 {
				sink(f, processor.Upstream)
			}
			return nil
		}
		p := procs[idx]
		emit := processor.Emit{
			Down: func(ff frames.Frame) {
				_ = processAt(idx+1, ff, processor.Downstream)
			},
			Up: func(ff frames.Frame) {
				_ = processAt(idx-1, ff, processor.Upstream)
			},
		}
		return p.Process(ctx, f, dir, emit)
	}
	return processAt(startIdx, f, dir)
}
