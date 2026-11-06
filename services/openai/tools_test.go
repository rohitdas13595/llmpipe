package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestParallelFragmentedToolCalls(t *testing.T) {
	var reqs atomic.Int32
	var mu sync.Mutex
	var bodies [][]byte
	first := sse(
		chunk(0, "call_weather", "get_weather", ""),
		chunk(1, "call_time", "get_time", ""),
		chunk(0, "", "", `{"location":`),
		chunk(1, "", "", `{"tz":`),
		chunk(0, "", "", `"Paris"}`),
		chunkFinish(1, "", "", `"UTC"}`, "tool_calls"),
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		n := reqs.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			_, _ = io.WriteString(w, first)
			return
		}
		_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"content":"Done."}}]}`+"\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()

	reg := tools.NewRegistry()
	release := make(chan struct{})
	var once sync.Once
	var inflight atomic.Int32
	var maxIn atomic.Int32
	handler := func(ctx context.Context, c tools.Call) (tools.Result, error) {
		n := inflight.Add(1)
		for {
			old := maxIn.Load()
			if n <= old || maxIn.CompareAndSwap(old, n) {
				break
			}
		}
		if inflight.Load() == 2 {
			once.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(time.Second):
			t.Errorf("handlers did not overlap")
		}
		inflight.Add(-1)
		return tools.Result{Content: string(c.Args)}, nil
	}
	reg.Register(tools.Function{Name: "get_weather", Description: "Weather"}, handler)
	reg.Register(tools.Function{Name: "get_time", Description: "Time"}, handler)

	ctxBag := aggregate.NewLLMContext("sys")
	ctxBag.AppendUser("weather and time")
	llm := NewLLMWithBaseURL("llm", "test", srv.URL, "gpt-test", ctxBag, nil)
	llm.httpClient = srv.Client()
	llm.SetRegistry(reg)
	asst := aggregate.NewAssistantAggregator("asst", ctxBag)
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(llm, asst))
	stop := startTask(t, task)
	defer stop()
	if err := task.QueueFrame(context.Background(), &frames.LLMRunFrame{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		msgs := ctxBag.Messages()
		if len(msgs) >= 6 && msgs[len(msgs)-1].Content == "Done." {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("context = %#v reqs=%d", ctxBag.Messages(), reqs.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	if reqs.Load() != 2 {
		t.Fatalf("model requests = %d, want one follow-up", reqs.Load())
	}
	if maxIn.Load() != 2 {
		t.Fatalf("max inflight = %d", maxIn.Load())
	}
	msgs := ctxBag.Messages()
	if msgs[2].Role != aggregate.RoleAssistant || len(msgs[2].ToolCalls) != 2 {
		t.Fatalf("assistant = %#v", msgs[2])
	}
	if msgs[2].ToolCalls[0].ID != "call_weather" || string(msgs[2].ToolCalls[0].Args) != `{"location":"Paris"}` {
		t.Fatalf("weather call = %#v", msgs[2].ToolCalls[0])
	}
	if msgs[2].ToolCalls[1].ID != "call_time" || string(msgs[2].ToolCalls[1].Args) != `{"tz":"UTC"}` {
		t.Fatalf("time call = %#v", msgs[2].ToolCalls[1])
	}
	if msgs[3].ToolCallID != "call_weather" || msgs[3].Content != `{"location":"Paris"}` {
		t.Fatalf("weather result = %#v", msgs[3])
	}
	if msgs[4].ToolCallID != "call_time" || msgs[4].Content != `{"tz":"UTC"}` {
		t.Fatalf("time result = %#v", msgs[4])
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) < 2 {
		t.Fatal("missing follow-up body")
	}
	var follow struct {
		Messages []map[string]any `json:"messages"`
		Tools    []any            `json:"tools"`
	}
	if err := json.Unmarshal(bodies[1], &follow); err != nil {
		t.Fatal(err)
	}
	if len(follow.Tools) != 2 {
		t.Fatalf("tools = %d", len(follow.Tools))
	}
	var toolIDs []string
	for _, m := range follow.Messages {
		if m["role"] == "tool" {
			id, _ := m["tool_call_id"].(string)
			toolIDs = append(toolIDs, id)
		}
	}
	if len(toolIDs) != 2 || toolIDs[0] != "call_weather" || toolIDs[1] != "call_time" {
		t.Fatalf("follow-up tool order = %v", toolIDs)
	}
}

func TestInterruptSlowToolHandler(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		reqs.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sse(chunkFinish(0, "call_slow", "slow", `{}`, "tool_calls")))
	}))
	defer srv.Close()
	started := make(chan struct{})
	reg := tools.NewRegistry()
	reg.Register(tools.Function{Name: "slow"}, func(ctx context.Context, c tools.Call) (tools.Result, error) {
		close(started)
		select {
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		case <-time.After(2 * time.Second):
			return tools.Result{Content: "late"}, nil
		}
	})
	ctxBag := aggregate.NewLLMContext("")
	ctxBag.AppendUser("go")
	llm := NewLLMWithBaseURL("llm", "test", srv.URL, "gpt-test", ctxBag, nil)
	llm.httpClient = srv.Client()
	llm.SetRegistry(reg)
	asst := aggregate.NewAssistantAggregator("asst", ctxBag)
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(llm, asst))
	stop := startTask(t, task)
	defer stop()
	if err := task.QueueFrame(context.Background(), &frames.LLMRunFrame{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	t0 := time.Now()
	if err := task.QueueFrame(context.Background(), &frames.InterruptionFrame{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		msgs := ctxBag.Messages()
		if len(msgs) >= 3 && msgs[2].Content == "CANCELLED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("context = %#v after %s", ctxBag.Messages(), time.Since(t0))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if time.Since(t0) >= time.Second {
		t.Fatal("cancellation waited on the 2s handler")
	}
	time.Sleep(40 * time.Millisecond)
	if reqs.Load() != 1 {
		t.Fatalf("requests = %d, want no follow-up", reqs.Load())
	}
}

func startTask(t *testing.T, task *pipeline.PipelineTask) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = task.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
	return func() {
		cancel()
		task.Cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}
}

func chunk(index int, id, name, args string) string {
	return chunkFinish(index, id, name, args, "")
}

func chunkFinish(index int, id, name, args, finish string) string {
	fn := map[string]any{}
	if name != "" {
		fn["name"] = name
	}
	if args != "" {
		fn["arguments"] = args
	}
	call := map[string]any{"index": index, "function": fn}
	if id != "" {
		call["id"] = id
		call["type"] = "function"
	}
	choice := map[string]any{"delta": map[string]any{"tool_calls": []any{call}}}
	if finish != "" {
		choice["finish_reason"] = finish
	}
	b, _ := json.Marshal(map[string]any{"choices": []any{choice}})
	return "data: " + string(b)
}

func sse(lines ...string) string {
	out := ""
	for _, ln := range lines {
		out += ln + "\n\n"
	}
	return out + "data: [DONE]\n\n"
}
