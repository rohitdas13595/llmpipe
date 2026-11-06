package llmbase

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestCallAccReassemblesIndexOrder(t *testing.T) {
	var a CallAcc
	a.Add(1, "call_time", "get_time", "")
	a.Add(0, "call_weather", "get_weather", `{"loca`)
	a.Add(0, "", "", `tion":"Paris"}`)
	a.Add(1, "", "", `{"tz":"UTC"}`)
	got := a.Calls()
	if len(got) != 2 || got[0].ID != "call_weather" || got[1].Name != "get_time" {
		t.Fatalf("calls = %#v", got)
	}
	if string(got[0].Args) != `{"location":"Paris"}` || string(got[1].Args) != `{"tz":"UTC"}` {
		t.Fatalf("args = %s | %s", got[0].Args, got[1].Args)
	}
}

func TestDecodeErrorBecomesResult(t *testing.T) {
	fn, h := tools.Typed("echo", "echo", func(ctx context.Context, a struct {
		N int `json:"n"`
	}) (any, error) {
		return a.N, nil
	})
	reg := tools.NewRegistry()
	reg.Register(fn, h)
	bag := &Bag{}
	bag.SetRegistry(reg)
	var got *frames.FunctionCallResultFrame
	Run(context.Background(), context.Background(), bag, nil, "", []aggregate.ToolCall{{
		ID: "c", Name: "echo", Args: json.RawMessage(`{`),
	}}, Hooks{Down: func(_ context.Context, f frames.Frame) error {
		if fr, ok := f.(*frames.FunctionCallResultFrame); ok {
			got = fr
		}
		return nil
	}})
	if got == nil || got.Cancelled || !strings.Contains(got.Result, "decode") {
		t.Fatalf("result = %#v", got)
	}
}

func TestInterruptSlowHandler(t *testing.T) {
	reg := tools.NewRegistry()
	bag := &Bag{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got *frames.FunctionCallResultFrame
	started := make(chan struct{})
	reg.Register(tools.Function{Name: "slow"}, func(ctx context.Context, c tools.Call) (tools.Result, error) {
		close(started)
		select {
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		case <-time.After(2 * time.Second):
			return tools.Result{Content: "late"}, nil
		}
	})
	bag.SetRegistry(reg)
	go func() {
		<-started
		cancel()
	}()
	t0 := time.Now()
	Run(ctx, context.Background(), bag, nil, "", []aggregate.ToolCall{{
		ID: "c", Name: "slow", Args: json.RawMessage(`{}`),
	}}, Hooks{Down: func(_ context.Context, f frames.Frame) error {
		if fr, ok := f.(*frames.FunctionCallResultFrame); ok {
			got = fr
		}
		return nil
	}})
	if time.Since(t0) >= time.Second {
		t.Fatal("slow handler was not cancelled")
	}
	if got == nil || !got.Cancelled || got.Result != "CANCELLED" {
		t.Fatalf("result = %#v", got)
	}
}

func TestTenParallelCalls(t *testing.T) {
	reg := tools.NewRegistry()
	var inflight atomic.Int32
	var max atomic.Int32
	reg.Register(tools.Function{Name: "n"}, func(ctx context.Context, c tools.Call) (tools.Result, error) {
		n := inflight.Add(1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		return tools.Result{Content: c.ID}, nil
	}, tools.Options{CancelOnInterruption: true, Timeout: time.Second})
	bag := &Bag{}
	bag.SetRegistry(reg)
	calls := make([]aggregate.ToolCall, 10)
	for i := range calls {
		calls[i] = aggregate.ToolCall{ID: string(rune('a' + i)), Name: "n", Args: json.RawMessage(`{}`)}
	}
	var mu sync.Mutex
	var results []string
	Run(context.Background(), context.Background(), bag, nil, "", calls, Hooks{
		Down: func(_ context.Context, f frames.Frame) error {
			if fr, ok := f.(*frames.FunctionCallResultFrame); ok {
				mu.Lock()
				results = append(results, fr.Result)
				mu.Unlock()
			}
			return nil
		},
	})
	if len(results) != 10 {
		t.Fatalf("results = %d", len(results))
	}
	if max.Load() < 2 {
		t.Fatalf("max inflight = %d, want parallel execution", max.Load())
	}
}
