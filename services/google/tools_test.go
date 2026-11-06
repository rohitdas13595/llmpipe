package google

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestParallelFunctionCalls(t *testing.T) {
	var reqs atomic.Int32
	llm := NewLLM("llm", "gemini-test", nil, nil, nil)
	ctxBag := aggregate.NewLLMContext("sys")
	ctxBag.AppendUser("both")
	llm.Ctx = ctxBag
	llm.generate = func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) iter.Seq2[*genai.GenerateContentResponse, error] {
		n := reqs.Add(1)
		return func(yield func(*genai.GenerateContentResponse, error) bool) {
			if n == 1 {
				if len(cfg.Tools) != 1 || len(cfg.Tools[0].FunctionDeclarations) != 2 {
					t.Errorf("tools = %#v", cfg.Tools)
				}
				_ = yield(&genai.GenerateContentResponse{Candidates: []*genai.Candidate{{
					Content: &genai.Content{Role: "model", Parts: []*genai.Part{
						{FunctionCall: &genai.FunctionCall{ID: "1", Name: "get_weather"}},
					}},
				}}}, nil)
				_ = yield(&genai.GenerateContentResponse{Candidates: []*genai.Candidate{{
					Content: &genai.Content{Role: "model", Parts: []*genai.Part{
						{FunctionCall: &genai.FunctionCall{ID: "1", Name: "get_weather", Args: map[string]any{"location": "Paris"}}},
						{FunctionCall: &genai.FunctionCall{ID: "2", Name: "get_time", Args: map[string]any{"tz": "UTC"}}},
					}},
				}}}, nil)
				return
			}
			_ = yield(&genai.GenerateContentResponse{Candidates: []*genai.Candidate{{
				Content: &genai.Content{Role: "model", Parts: []*genai.Part{{Text: "Done."}}},
			}}}, nil)
		}
	}
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
	reg.Register(tools.Function{Name: "get_weather", Description: "Weather", Parameters: map[string]any{"type": "object"}}, handler)
	reg.Register(tools.Function{Name: "get_time", Description: "Time", Parameters: map[string]any{"type": "object"}}, handler)
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
	deadline := time.Now().Add(3 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
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
	deadline = time.Now().Add(3 * time.Second)
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
	if msgs[2].ToolCalls[0].ID != "1" || string(msgs[2].ToolCalls[0].Args) != `{"location":"Paris"}` {
		t.Fatalf("weather = %#v", msgs[2].ToolCalls[0])
	}
	if msgs[2].ToolCalls[1].ID != "2" || string(msgs[2].ToolCalls[1].Args) != `{"tz":"UTC"}` {
		t.Fatalf("time = %#v", msgs[2].ToolCalls[1])
	}
	if msgs[3].Content != `{"location":"Paris"}` || msgs[4].Content != `{"tz":"UTC"}` {
		t.Fatalf("results %#v %#v", msgs[3], msgs[4])
	}
}
