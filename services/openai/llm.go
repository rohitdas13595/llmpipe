package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	oa "github.com/rohitdas13595/llmpipe/adapters/openai"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/llmbase"
)

// LLM streams chat completions via the OpenAI-compatible Chat Completions API
// (Pipecat: OpenAILLMService; set BaseURL to https://api.groq.com/openai/v1 for Groq).
type LLM struct {
	name    string
	APIKey  string
	BaseURL string // e.g. https://api.openai.com/v1 (default) or Groq/Together base
	// APIVersion is optional; when set (e.g. Azure OpenAI), appended as ?api-version= on chat completions.
	APIVersion string
	Model      string
	Ctx        *aggregate.LLMContext
	Reenter    services.ReenterFunc
	httpClient *http.Client
	tools      *llmbase.Bag

	services.Slot
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewLLM(name, apiKey, model string, c *aggregate.LLMContext, reenter services.ReenterFunc) *LLM {
	if model == "" {
		model = "gpt-4o-mini"
	}
	base := os.Getenv("OPENAI_BASE_URL")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	return &LLM{
		name:       name,
		APIKey:     apiKey,
		BaseURL:    strings.TrimSuffix(base, "/"),
		Model:      model,
		Ctx:        c,
		Reenter:    reenter,
		httpClient: http.DefaultClient,
		tools:      &llmbase.Bag{},
	}
}

// NewLLMWithBaseURL is like NewLLM but forces a base URL (Pipecat-style per-provider clients).
func NewLLMWithBaseURL(name, apiKey, baseURL, model string, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if baseURL == "" {
		return NewLLM(name, apiKey, model, c, re)
	}
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &LLM{
		name:       name,
		APIKey:     apiKey,
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		Model:      model,
		Ctx:        c,
		Reenter:    re,
		httpClient: http.DefaultClient,
		tools:      &llmbase.Bag{},
	}
}

func (l *LLM) Name() string { return l.name }

// SetRegistry installs function handlers. The same registry serves Groq, Azure,
// and the other OpenAI-compatible backends because they share this client.
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
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("openai: pipeline handle not configured")})
			return nil
		}
		services.PipelineLog("llm", "%s: LLMRunFrame → starting chat completion", l.name)
		go l.runCompletion(l.TurnContext())
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

type chatReq struct {
	Model    string `json:"model"`
	Messages any    `json:"messages"`
	Stream   bool   `json:"stream"`
	Tools    any    `json:"tools,omitempty"`
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

func (l *LLM) runCompletion(parent context.Context) {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.mu.Unlock()
	defer cancel()

	wire, err := oa.Chat{}.Messages(l.Ctx.Messages())
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	schema := l.tools.Schema()
	reqBody := chatReq{Model: l.Model, Messages: wire, Stream: true}
	if len(schema.Functions) > 0 {
		toolsWire, err := oa.Chat{}.Tools(schema)
		if err != nil {
			_ = l.push(ctx, &frames.ErrorFrame{Err: err})
			return
		}
		reqBody.Tools = toolsWire
	}
	services.PipelineLog("llm", "%s: POST /v1/chat/completions model=%q messages=%d tools=%d", l.name, l.Model, len(l.Ctx.Messages()), len(schema.Functions))
	body, err := json.Marshal(reqBody)
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	base := l.BaseURL
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	chatURL := strings.TrimSuffix(base, "/") + "/chat/completions"
	if l.APIVersion != "" {
		u, err := url.Parse(chatURL)
		if err == nil {
			q := u.Query()
			q.Set("api-version", l.APIVersion)
			u.RawQuery = q.Encode()
			chatURL = u.String()
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, chatURL, bytes.NewReader(body))
	if err != nil {
		services.PipelineLog("llm", "%s: request error: %v", l.name, err)
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("Authorization", "Bearer "+l.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		services.PipelineLog("llm", "%s: HTTP do error: %v", l.name, err)
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		services.PipelineLog("llm", "%s: API %s body=%s", l.name, resp.Status, string(b))
		_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("openai: %s: %s", resp.Status, string(b))})
		return
	}

	_ = l.push(ctx, &frames.LLMFullResponseStartFrame{})
	var spoken strings.Builder
	var acc llmbase.CallAcc
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if ctx.Err() != nil {
			services.PipelineLog("llm", "%s: stream cancelled (%v)", l.name, ctx.Err())
			_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
			return
		}
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var ch streamChunk
		if json.Unmarshal([]byte(data), &ch) != nil || len(ch.Choices) == 0 {
			continue
		}
		d := ch.Choices[0].Delta
		if d.Content != "" {
			spoken.WriteString(d.Content)
			if os.Getenv("LLMPIPE_LOG_LLM_TOKENS") == "1" {
				services.PipelineLog("llm", "%s: stream token: %q", l.name, d.Content)
			}
			_ = l.push(ctx, &frames.LLMTextFrame{Text: d.Content})
		}
		for _, tc := range d.ToolCalls {
			acc.Add(tc.Index, tc.ID, tc.Function.Name, tc.Function.Arguments)
		}
	}
	calls := acc.Calls()
	if len(calls) == 0 || ctx.Err() != nil {
		_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
		services.PipelineLog("llm", "%s: stream finished", l.name)
		return
	}
	services.PipelineLog("llm", "%s: %d tool call(s)", l.name, len(calls))
	llmbase.Run(parent, l.ProcContext(), l.tools, l.Ctx, spoken.String(), calls, l.hooks(func() {
		_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
	}))
	services.PipelineLog("llm", "%s: tool calls finished", l.name)
}

func (l *LLM) hooks(before func()) llmbase.Hooks {
	return llmbase.Hooks{
		BeforeHandlers: before,
		Down:           func(ctx context.Context, f frames.Frame) error { return l.push(ctx, f) },
		Up: func(ctx context.Context, f frames.Frame) error {
			if l.H() == nil {
				return nil
			}
			return services.PushUp(ctx, l.H(), l.name, f)
		},
	}
}
