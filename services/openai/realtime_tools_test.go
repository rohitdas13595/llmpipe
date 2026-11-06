package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestRealtimeFunctionCallWebSocket(t *testing.T) {
	var mu sync.Mutex
	var got []map[string]any
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, _, err = c.ReadMessage()
		if err != nil {
			return
		}
		_ = c.WriteJSON(map[string]any{
			"type": "response.function_call_arguments.delta", "call_id": "call_1", "delta": `{"location":`,
		})
		_ = c.WriteJSON(map[string]any{
			"type": "response.function_call_arguments.done", "call_id": "call_1", "name": "get_weather", "arguments": `{"location":"Paris"}`,
		})
		_ = c.WriteJSON(map[string]any{
			"type": "response.function_call_arguments.done", "call_id": "call_2", "name": "get_time", "arguments": `{"tz":"UTC"}`,
		})
		_ = c.WriteJSON(map[string]any{"type": "response.done"})
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			var m map[string]any
			if err := c.ReadJSON(&m); err != nil {
				return
			}
			mu.Lock()
			got = append(got, m)
			n := len(got)
			mu.Unlock()
			if n >= 3 {
				return
			}
		}
	}))
	defer srv.Close()

	reg := tools.NewRegistry()
	reg.Register(tools.Function{Name: "get_weather", Description: "Weather", Parameters: map[string]any{"type": "object"}},
		func(ctx context.Context, c tools.Call) (tools.Result, error) {
			return tools.Result{Content: `{"condition":"sun"}`}, nil
		})
	reg.Register(tools.Function{Name: "get_time", Description: "Time", Parameters: map[string]any{"type": "object"}},
		func(ctx context.Context, c tools.Call) (tools.Result, error) {
			return tools.Result{Content: `{"time":"noon"}`}, nil
		})
	rt := NewRealtime("rt", "test-key", "gpt-test", "be nice", func(ctx context.Context, name string, f frames.Frame) error {
		return nil
	}, nil)
	rt.endpoint = "ws" + strings.TrimPrefix(srv.URL, "http")
	rt.SetRegistry(reg)
	rt.connect(context.Background())

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			mu.Lock()
			t.Fatalf("client messages = %#v", got)
			mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
	rt.closeConn()
	mu.Lock()
	defer mu.Unlock()
	var outputs, creates int
	for _, m := range got {
		switch m["type"] {
		case "conversation.item.create":
			outputs++
			item, _ := m["item"].(map[string]any)
			if item["type"] != "function_call_output" {
				t.Fatalf("item = %#v", item)
			}
		case "response.create":
			creates++
		}
	}
	if outputs != 2 || creates != 1 {
		t.Fatalf("outputs=%d creates=%d msgs=%#v", outputs, creates, got)
	}
}
