// Package anthropic provides Claude Messages API streaming (Pipecat: AnthropicLLMService).
package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	anth "github.com/rohitdas13595/llmpipe/adapters/anthropic"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/llmbase"
)

// LLM streams Claude via Messages API (streaming).
type LLM struct {
	name        string
	APIKey      string
	Model       string
	Ctx         *aggregate.LLMContext
	Reenter     services.ReenterFunc
	httpClient  *http.Client
	messagesURL string
	tools       *llmbase.Bag

	services.Slot
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewLLM(name, apiKey, model string, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if model == "" {
		model = "claude-3-5-haiku-20241022"
	}
	return &LLM{
		name: name, APIKey: apiKey, Model: model, Ctx: c, Reenter: re,
		httpClient: http.DefaultClient, tools: &llmbase.Bag{},
		messagesURL: "https://api.anthropic.com/v1/messages",
	}
}

func (l *LLM) Name() string { return l.name }

// SetRegistry installs function handlers for Claude tool_use blocks.
func (l *LLM) SetRegistry(r *tools.Registry) {
	if l.tools == nil {
		l.tools = &llmbase.Bag{}
	}
	l.tools.SetRegistry(r)
}

func (l *LLM) push(ctx context.Context, f frames.Frame) error {
	return services.PushDown(ctx, l.H(), l.Reenter, l.name, f)
}

func (l *LLM) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if _, ok := f.(*frames.LLMRunFrame); ok {
		if l.H() == nil && l.Reenter == nil {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("anthropic: pipeline handle not configured")})
			return nil
		}
		services.PipelineLog("llm", "%s: LLMRunFrame → Claude stream", l.name)
		go l.runStream(l.TurnContext())
		return nil
	}
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
	case *frames.StartFrame, *frames.LLMFullResponseStartFrame, *frames.LLMFullResponseEndFrame,
		*frames.LLMTextFrame, *frames.TextFrame:
		emit.Down(f)
	case *frames.InterruptionFrame, *frames.CancelFrame:
		l.mu.Lock()
		if l.cancel != nil {
			l.cancel()
			l.cancel = nil
		}
		l.mu.Unlock()
		emit.Down(f)
	case *frames.LLMSetToolsFrame:
		if l.tools != nil {
			l.tools.SetSchema(fr.Tools)
		}
		emit.Down(f)
	case *frames.ErrorFrame:
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

type msgReq struct {
	Model     string `json:"model"`
	MaxTokens int    `json:"max_tokens"`
	System    string `json:"system,omitempty"`
	Messages  any    `json:"messages"`
	Tools     any    `json:"tools,omitempty"`
	Stream    bool   `json:"stream"`
}

func (l *LLM) runStream(parent context.Context) {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.mu.Unlock()
	defer cancel()

	raw, err := anth.Messages{}.Messages(l.Ctx.Messages())
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	prompt, ok := raw.(anth.Prompt)
	if !ok {
		_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("anthropic: bad prompt")})
		return
	}
	schema := l.tools.Schema()
	bodyReq := msgReq{
		Model:     l.Model,
		MaxTokens: envInt("ANTHROPIC_MAX_TOKENS", 1024),
		System:    prompt.System,
		Messages:  prompt.Messages,
		Stream:    true,
	}
	if len(schema.Functions) > 0 {
		toolWire, err := anth.Messages{}.Tools(schema)
		if err != nil {
			_ = l.push(ctx, &frames.ErrorFrame{Err: err})
			return
		}
		bodyReq.Tools = toolWire
	}
	body, err := json.Marshal(bodyReq)
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	endpoint := l.messagesURL
	if endpoint == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("x-api-key", l.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("anthropic: %s: %s", resp.Status, string(b))})
		return
	}

	_ = l.push(ctx, &frames.LLMFullResponseStartFrame{})
	var spoken strings.Builder
	var acc llmbase.CallAcc
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if ctx.Err() != nil {
			break
		}
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var ev struct {
			Type         string `json:"type"`
			Index        int    `json:"index"`
			ContentBlock struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"content_block"`
			Delta struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		switch ev.Type {
		case "content_block_start":
			if ev.ContentBlock.Type == "tool_use" {
				acc.Add(ev.Index, ev.ContentBlock.ID, ev.ContentBlock.Name, "")
			}
		case "content_block_delta":
			switch ev.Delta.Type {
			case "text_delta":
				if ev.Delta.Text != "" {
					spoken.WriteString(ev.Delta.Text)
					_ = l.push(ctx, &frames.LLMTextFrame{Text: ev.Delta.Text})
				}
			case "input_json_delta":
				acc.Add(ev.Index, "", "", ev.Delta.PartialJSON)
			}
		}
	}
	calls := acc.Calls()
	if len(calls) == 0 || ctx.Err() != nil {
		_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
		services.PipelineLog("llm", "%s: Claude stream finished", l.name)
		return
	}
	services.PipelineLog("llm", "%s: %d tool call(s)", l.name, len(calls))
	llmbase.Run(parent, l.ProcContext(), l.tools, l.Ctx, spoken.String(), calls, llmbase.Hooks{
		BeforeHandlers: func() {
			_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
		},
		Down: func(ctx context.Context, f frames.Frame) error { return l.push(ctx, f) },
		Up: func(ctx context.Context, f frames.Frame) error {
			if l.H() == nil {
				return nil
			}
			return services.PushUp(ctx, l.H(), l.name, f)
		},
	})
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}
