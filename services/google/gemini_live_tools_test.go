package google

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

type fakeLive struct {
	mu    sync.Mutex
	sent  []genai.LiveToolResponseInput
	ready chan struct{}
}

func (f *fakeLive) SendRealtimeInput(genai.LiveRealtimeInput) error { return nil }
func (f *fakeLive) SendToolResponse(in genai.LiveToolResponseInput) error {
	f.mu.Lock()
	f.sent = append(f.sent, in)
	f.mu.Unlock()
	select {
	case <-f.ready:
	default:
		close(f.ready)
	}
	return nil
}
func (f *fakeLive) Receive() (*genai.LiveServerMessage, error) {
	return nil, context.Canceled
}
func (f *fakeLive) Close() error { return nil }

func TestGeminiLiveToolResponse(t *testing.T) {
	reg := tools.NewRegistry()
	release := make(chan struct{})
	var once sync.Once
	var inflight int
	var mu sync.Mutex
	handler := func(ctx context.Context, c tools.Call) (tools.Result, error) {
		mu.Lock()
		inflight++
		if inflight == 2 {
			once.Do(func() { close(release) })
		}
		mu.Unlock()
		select {
		case <-release:
		case <-time.After(time.Second):
			t.Errorf("handlers did not overlap")
		}
		return tools.Result{Content: `{"ok":true,"name":"` + c.Name + `"}`}, nil
	}
	reg.Register(tools.Function{Name: "get_weather", Description: "Weather", Parameters: map[string]any{"type": "object"}}, handler)
	reg.Register(tools.Function{Name: "get_time", Description: "Time", Parameters: map[string]any{"type": "object"}}, handler)

	fake := &fakeLive{ready: make(chan struct{})}
	g := NewGeminiLive("g", nil, "gemini-live", "be nice", func(context.Context, string, frames.Frame) error { return nil }, nil)
	g.SetRegistry(reg)
	g.session = fake
	g.handleLive(context.Background(), &genai.LiveServerMessage{
		ToolCall: &genai.LiveServerToolCall{FunctionCalls: []*genai.FunctionCall{
			{ID: "1", Name: "get_weather", Args: map[string]any{"location": "Paris"}},
			{ID: "2", Name: "get_time", Args: map[string]any{"tz": "UTC"}},
		}},
	})
	select {
	case <-fake.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("no tool response")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.sent) != 1 || len(fake.sent[0].FunctionResponses) != 2 {
		t.Fatalf("responses = %#v", fake.sent)
	}
	if fake.sent[0].FunctionResponses[0].Name != "get_weather" || fake.sent[0].FunctionResponses[1].Name != "get_time" {
		t.Fatalf("order = %#v", fake.sent[0].FunctionResponses)
	}
}
