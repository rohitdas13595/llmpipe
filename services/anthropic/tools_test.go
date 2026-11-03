package anthropic

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

func TestParallelFragmentedToolUse(t *testing.T) {
	var reqs atomic.Int32
	first := anthSSE(
		anthBlockStart(0, "toolu_1", "get_weather"),
		anthBlockStart(1, "toolu_2", "get_time"),
		anthPartial(0, `{"location":`),
		anthPartial(1, `{"tz":`),
		anthPartial(0, `"Paris"}`),
		anthPartial(1, `"UTC"}`),
		anthStop(),
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		n := reqs.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			_, _ = io.WriteString(w, first)
			return
		}
		_, _ = io.WriteString(w, anthSSE(anthText("Done.")))
	}))
	defer srv.Close()

	reg := tools.NewRegistry()
	release := make(chan struct{})
	var once sync.Once
	var inflight atomic.Int32
	handler := func(ctx context.Context, c tools.Call) (tools.Result, error) {
		if inflight.Add(1) == 2 {
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
	ctxBag.AppendUser("both")
	llm := NewLLM("llm", "test", "claude-test", ctxBag, nil)
	llm.httpClient = srv.Client()
	llm.messagesURL = srv.URL
	llm.SetRegistry(reg)
	asst := aggregate.NewAssistantAggregator("asst", ctxBag)
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(llm, asst))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = task.Run(ctx)
		close(done)
	}()
	waitRunning(t, task)
	defer func() {
		cancel()
		task.Cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}()
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
			t.Fatalf("context=%#v reqs=%d", ctxBag.Messages(), reqs.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	if reqs.Load() != 2 {
		t.Fatalf("requests = %d", reqs.Load())
	}
	msgs := ctxBag.Messages()
	if string(msgs[2].ToolCalls[0].Args) != `{"location":"Paris"}` || string(msgs[2].ToolCalls[1].Args) != `{"tz":"UTC"}` {
		t.Fatalf("calls = %#v", msgs[2].ToolCalls)
	}
	if msgs[3].ToolCallID != "toolu_1" || msgs[4].ToolCallID != "toolu_2" {
		t.Fatalf("results = %#v %#v", msgs[3], msgs[4])
	}
}

func waitRunning(t *testing.T, task *pipeline.PipelineTask) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

func anthBlockStart(index int, id, name string) string {
	b, _ := json.Marshal(map[string]any{
		"type": "content_block_start", "index": index,
		"content_block": map[string]any{"type": "tool_use", "id": id, "name": name, "input": map[string]any{}},
	})
	return "data: " + string(b)
}

func anthPartial(index int, frag string) string {
	b, _ := json.Marshal(map[string]any{
		"type": "content_block_delta", "index": index,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": frag},
	})
	return "data: " + string(b)
}

func anthText(text string) string {
	b, _ := json.Marshal(map[string]any{
		"type": "content_block_delta", "index": 0,
		"delta": map[string]any{"type": "text_delta", "text": text},
	})
	return "data: " + string(b)
}

func anthStop() string {
	return `data: {"type":"message_delta","delta":{"stop_reason":"tool_use"}}`
}

func anthSSE(lines ...string) string {
	out := ""
	for _, ln := range lines {
		out += ln + "\n\n"
	}
	return out
}
