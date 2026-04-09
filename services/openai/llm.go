package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// LLM streams chat completions via the OpenAI API.
type LLM struct {
	name       string
	APIKey     string
	Model      string
	Ctx        *aggregate.LLMContext
	Reenter    services.ReenterFunc
	httpClient *http.Client

	mu        sync.Mutex
	cancel    context.CancelFunc
	activeGen int64
	runGen    int64
}

func NewLLM(name, apiKey, model string, c *aggregate.LLMContext, reenter services.ReenterFunc) *LLM {
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &LLM{
		name:       name,
		APIKey:     apiKey,
		Model:      model,
		Ctx:        c,
		Reenter:    reenter,
		httpClient: http.DefaultClient,
	}
}

func (l *LLM) Name() string { return l.name }

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
		l.runGen++
		l.mu.Unlock()
		emit.Down(f)
	case *frames.LLMRunFrame:
		if l.Reenter == nil {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("openai: Reenter not configured")})
			return nil
		}
		log.Printf("%s: LLMRunFrame → starting chat completion", l.name)
		go l.runCompletion(context.Background())
	case *frames.ErrorFrame:
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

type chatReq struct {
	Model    string           `json:"model"`
	Messages []map[string]any `json:"messages"`
	Stream   bool             `json:"stream"`
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

func (l *LLM) runCompletion(bg context.Context) {
	l.mu.Lock()
	l.runGen++
	gen := l.runGen
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(bg)
	l.cancel = cancel
	l.activeGen = gen
	l.mu.Unlock()

	defer func() {
		cancel()
		l.mu.Lock()
		if l.activeGen == gen {
			l.cancel = nil
		}
		l.mu.Unlock()
	}()

	msgs := l.Ctx.Snapshot()
	log.Printf("%s: POST /v1/chat/completions model=%q messages=%d", l.name, l.Model, len(msgs))
	body, _ := json.Marshal(chatReq{Model: l.Model, Messages: msgs, Stream: true})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.openai.com/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		log.Printf("%s: request error: %v", l.name, err)
		_ = l.Reenter(ctx, l.name, &frames.ErrorFrame{Err: err})
		return
	}
	req.Header.Set("Authorization", "Bearer "+l.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		log.Printf("%s: HTTP do error: %v", l.name, err)
		_ = l.Reenter(ctx, l.name, &frames.ErrorFrame{Err: err})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("%s: API %s body=%s", l.name, resp.Status, string(b))
		_ = l.Reenter(ctx, l.name, &frames.ErrorFrame{Err: fmt.Errorf("openai: %s: %s", resp.Status, string(b))})
		return
	}

	_ = l.Reenter(ctx, l.name, &frames.LLMFullResponseStartFrame{})
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if ctx.Err() != nil {
			log.Printf("%s: stream cancelled (%v)", l.name, ctx.Err())
			_ = l.Reenter(ctx, l.name, &frames.LLMFullResponseEndFrame{})
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
		if json.Unmarshal([]byte(data), &ch) != nil {
			continue
		}
		if len(ch.Choices) > 0 && ch.Choices[0].Delta.Content != "" {
			_ = l.Reenter(ctx, l.name, &frames.LLMTextFrame{Text: ch.Choices[0].Delta.Content})
		}
	}
	_ = l.Reenter(ctx, l.name, &frames.LLMFullResponseEndFrame{})
	log.Printf("%s: stream finished", l.name)
}
