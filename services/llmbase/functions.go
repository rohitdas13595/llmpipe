// Package llmbase runs function-call handlers for chat and realtime models.
package llmbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// Bag is the tool registry and an optional schema override from LLMSetToolsFrame.
type Bag struct {
	mu       sync.Mutex
	reg      *tools.Registry
	override *tools.Schema
}

// SetRegistry installs the handlers and the default schema.
func (b *Bag) SetRegistry(r *tools.Registry) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.reg = r
	b.mu.Unlock()
}

// SetSchema replaces the schema sent to the model. Handlers still come from the registry.
func (b *Bag) SetSchema(s tools.Schema) {
	if b == nil {
		return
	}
	cp := s
	b.mu.Lock()
	b.override = &cp
	b.mu.Unlock()
}

// Schema is the declaration list for the next model request.
func (b *Bag) Schema() tools.Schema {
	if b == nil {
		return tools.Schema{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.override != nil {
		return *b.override
	}
	if b.reg != nil {
		return b.reg.Schema()
	}
	return tools.Schema{}
}

// Registry returns the handler registry, which may be nil.
func (b *Bag) Registry() *tools.Registry {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reg
}

// Hooks receives the frames and callbacks for one batch.
// BeforeHandlers runs after the started frames are pushed and before handlers
// run, so a chat service can close the model response first.
// OnResult and OnBatchDone are for realtime transports that must answer the
// socket themselves. Chat follow-up is the assistant aggregator's job.
type Hooks struct {
	Down           func(context.Context, frames.Frame) error
	Up             func(context.Context, frames.Frame) error
	BeforeHandlers func()
	OnResult       func(call aggregate.ToolCall, res tools.Result, cancelled bool)
	OnBatchDone    func(cancelled bool)
}

// CallAcc reassembles tool-call fragments keyed by the provider's index.
type CallAcc struct {
	by map[int]*aggregate.ToolCall
}

// Add appends one streamed fragment. Empty id, name, or arguments are ignored
// so later chunks do not wipe earlier ones.
func (a *CallAcc) Add(index int, id, name, frag string) {
	if a.by == nil {
		a.by = map[int]*aggregate.ToolCall{}
	}
	tc := a.by[index]
	if tc == nil {
		tc = &aggregate.ToolCall{}
		a.by[index] = tc
	}
	if id != "" {
		tc.ID = id
	}
	if name != "" {
		tc.Name = name
	}
	if frag != "" {
		tc.Args = append(tc.Args, frag...)
	}
}

// Calls returns the accumulated calls in index order.
func (a *CallAcc) Calls() []aggregate.ToolCall {
	if a == nil || len(a.by) == 0 {
		return nil
	}
	idx := make([]int, 0, len(a.by))
	for i := range a.by {
		idx = append(idx, i)
	}
	sort.Ints(idx)
	out := make([]aggregate.ToolCall, 0, len(idx))
	for _, i := range idx {
		tc := *a.by[i]
		if tc.ID == "" {
			tc.ID = fmt.Sprintf("call_%d", i)
		}
		if len(tc.Args) == 0 {
			tc.Args = json.RawMessage(`{}`)
		} else {
			tc.Args = append(json.RawMessage(nil), tc.Args...)
		}
		out = append(out, tc)
	}
	return out
}

// Run pushes the started frames, runs handlers in parallel, then pushes one
// result per call. turn is cancelled on interruption when the tool opted in.
// proc outlives the turn and is used otherwise. Pushes use a background
// context so a cancelled turn still delivers uninterruptible results.
func Run(turn, proc context.Context, bag *Bag, conv any, content string, calls []aggregate.ToolCall, h Hooks) {
	if len(calls) == 0 {
		return
	}
	if turn == nil {
		turn = context.Background()
	}
	if proc == nil {
		proc = context.Background()
	}
	batch := make([]tools.Call, len(calls))
	for i, c := range calls {
		batch[i] = tools.Call{
			ID: c.ID, Name: c.Name,
			Args:    append(json.RawMessage(nil), c.Args...),
			Context: conv,
		}
	}
	bg := context.Background()
	started := &frames.FunctionCallsStartedFrame{Content: content, Batch: batch}
	push(h.Down, bg, started)
	push(h.Up, bg, &frames.FunctionCallsStartedFrame{Content: content, Batch: append([]tools.Call(nil), batch...)})
	for _, c := range batch {
		push(h.Down, bg, &frames.FunctionCallInProgressFrame{CallID: c.ID, Name: c.Name, Args: c.Args})
	}
	if h.BeforeHandlers != nil {
		h.BeforeHandlers()
	}
	reg := bag.Registry()
	var wg sync.WaitGroup
	var anyCancel atomic.Bool
	for _, c := range calls {
		wg.Add(1)
		go func(c aggregate.ToolCall) {
			defer wg.Done()
			res, cancelled := exec(turn, proc, reg, conv, c)
			if cancelled {
				anyCancel.Store(true)
				push(h.Up, bg, &frames.FunctionCallCancelFrame{CallID: c.ID})
			} else {
				push(h.Up, bg, &frames.FunctionCallResultFrame{
					CallID: c.ID, Name: c.Name, Result: res.Content, RunLLM: res.RunLLM,
				})
			}
			push(h.Down, bg, &frames.FunctionCallResultFrame{
				CallID: c.ID, Name: c.Name, Result: res.Content, RunLLM: res.RunLLM, Cancelled: cancelled,
			})
			if h.OnResult != nil {
				h.OnResult(c, res, cancelled)
			}
		}(c)
	}
	wg.Wait()
	if h.OnBatchDone != nil {
		h.OnBatchDone(anyCancel.Load())
	}
}

func push(fn func(context.Context, frames.Frame) error, ctx context.Context, f frames.Frame) {
	if fn == nil || f == nil {
		return
	}
	_ = fn(ctx, f)
}

func exec(turn, proc context.Context, reg *tools.Registry, conv any, c aggregate.ToolCall) (res tools.Result, cancelled bool) {
	defer func() {
		if rec := recover(); rec != nil {
			res = tools.Result{Content: errorContent(fmt.Errorf("panic: %v", rec))}
			cancelled = false
		}
	}()
	var h tools.Handler
	opt := tools.Options{CancelOnInterruption: true, Timeout: tools.DefaultTimeout}
	if reg != nil {
		if _, handler, o, ok := reg.Lookup(c.Name); ok {
			h = handler
			opt = o
		}
	}
	if h == nil {
		return tools.Result{Content: errorContent(fmt.Errorf("unknown tool %q", c.Name))}, false
	}
	base := proc
	if opt.CancelOnInterruption {
		base = turn
	}
	ctx := base
	if opt.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(base, opt.Timeout)
		defer cancel()
	}
	out, err := h(ctx, tools.Call{ID: c.ID, Name: c.Name, Args: c.Args, Context: conv})
	if ctx.Err() != nil && opt.CancelOnInterruption && errors.Is(ctx.Err(), context.Canceled) {
		return tools.Result{Content: "CANCELLED"}, true
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return tools.Result{Content: errorContent(fmt.Errorf("timeout"))}, false
	}
	if err != nil {
		return tools.Result{Content: errorContent(err)}, false
	}
	return out, false
}

func errorContent(err error) string {
	b, mErr := json.Marshal(map[string]string{"error": err.Error()})
	if mErr != nil {
		return `{"error":"unknown"}`
	}
	return string(b)
}

// CancelGroup is a replaceable cancel func for realtime tool batches.
type CancelGroup struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

// Replace cancels the previous batch and returns a context under parent.
func (c *CancelGroup) Replace(parent context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	return ctx
}

// Cancel aborts the current batch.
func (c *CancelGroup) Cancel() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}
