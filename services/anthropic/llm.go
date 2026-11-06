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

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// LLM streams Claude via Messages API (streaming).
type LLM struct {
	name       string
	APIKey     string
	Model      string
	Ctx        *aggregate.LLMContext
	Reenter    services.ReenterFunc
	httpClient *http.Client

	services.Slot
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewLLM(name, apiKey, model string, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if model == "" {
		model = "claude-3-5-haiku-20241022"
	}
	return &LLM{name: name, APIKey: apiKey, Model: model, Ctx: c, Reenter: re, httpClient: http.DefaultClient}
}

func (l *LLM) Name() string { return l.name }

func (l *LLM) push(ctx context.Context, f frames.Frame) error {
	return services.PushDown(ctx, l.H(), l.Reenter, l.name, f)
}

func (l *LLM) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch f.(type) {
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
	case *frames.LLMRunFrame:
		if l.H() == nil && l.Reenter == nil {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("anthropic: pipeline handle not configured")})
			return nil
		}
		services.PipelineLog("llm", "%s: LLMRunFrame → Claude stream", l.name)
		go l.runStream(l.TurnContext())
	case *frames.ErrorFrame:
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

type msgReq struct {
	Model     string              `json:"model"`
	MaxTokens int                 `json:"max_tokens"`
	System    string              `json:"system,omitempty"`
	Messages  []map[string]string `json:"messages"`
	Stream    bool                `json:"stream"`
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

	msgs := l.Ctx.Snapshot()
	var sys string
	var conv []map[string]string
	for _, m := range msgs {
		role, _ := m["role"].(string)
		content, _ := m["content"].(string)
		if role == "system" {
			sys = content
			continue
		}
		r := role
		if r == "assistant" {
			r = "assistant"
		} else {
			r = "user"
		}
		conv = append(conv, map[string]string{"role": r, "content": content})
	}
	body, _ := json.Marshal(msgReq{
		Model:     l.Model,
		MaxTokens: envInt("ANTHROPIC_MAX_TOKENS", 1024),
		System:    sys,
		Messages:  conv,
		Stream:    true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
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
			Type  string `json:"type"`
			Delta struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"delta"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		if ev.Type == "content_block_delta" && ev.Delta.Text != "" {
			_ = l.push(ctx, &frames.LLMTextFrame{Text: ev.Delta.Text})
		}
	}
	_ = l.push(ctx, &frames.LLMFullResponseEndFrame{})
	services.PipelineLog("llm", "%s: Claude stream finished", l.name)
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
