package google

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// LLM uses Google GenAI (Gemini) streaming.
type LLM struct {
	name    string
	Client  *genai.Client
	Model   string
	Ctx     *aggregate.LLMContext
	Reenter services.ReenterFunc

	mu        sync.Mutex
	cancel    context.CancelFunc
	activeGen int64
	runGen    int64
}

func NewLLM(name, model string, client *genai.Client, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if model == "" {
		model = "gemini-2.0-flash"
	}
	return &LLM{name: name, Client: client, Model: model, Ctx: c, Reenter: re}
}

func (l *LLM) Name() string { return l.name }

func (l *LLM) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch f.(type) {
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
		if l.Client == nil || l.Reenter == nil {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("google llm: client or Reenter nil")})
			return nil
		}
		log.Printf("%s: LLMRunFrame → Gemini stream model=%q", l.name, l.Model)
		go l.stream(context.Background())
	default:
		emit.Down(f)
	}
	return nil
}

func (l *LLM) stream(bg context.Context) {
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
	var sys *genai.Content
	var contents []*genai.Content
	for _, m := range msgs {
		role, _ := m["role"].(string)
		content, _ := m["content"].(string)
		if role == "system" {
			sys = &genai.Content{Parts: []*genai.Part{{Text: content}}}
			continue
		}
		r := genai.RoleUser
		if role == "assistant" {
			r = genai.RoleModel
		}
		contents = append(contents, &genai.Content{Role: r, Parts: []*genai.Part{{Text: content}}})
	}
	cfg := &genai.GenerateContentConfig{}
	if sys != nil {
		cfg.SystemInstruction = sys
	}

	log.Printf("%s: GenerateContentStream model=%q contents=%d", l.name, l.Model, len(contents))
	_ = l.Reenter(ctx, l.name, &frames.LLMFullResponseStartFrame{})
	stream := l.Client.Models.GenerateContentStream(ctx, l.Model, contents, cfg)
	textChunks := 0
	for resp, err := range stream {
		if ctx.Err() != nil {
			log.Printf("%s: stream cancelled (%v)", l.name, ctx.Err())
			break
		}
		if err != nil {
			log.Printf("%s: stream error: %v", l.name, err)
			_ = l.Reenter(ctx, l.name, &frames.ErrorFrame{Err: err})
			break
		}
		if len(resp.Candidates) == 0 {
			if resp.PromptFeedback != nil {
				log.Printf("%s: no candidates (prompt_feedback=%+v)", l.name, resp.PromptFeedback)
			}
			continue
		}
		for _, cand := range resp.Candidates {
			if cand.FinishReason != "" && cand.FinishReason != genai.FinishReasonStop {
				log.Printf("%s: candidate finish_reason=%s", l.name, cand.FinishReason)
			}
			if cand.Content == nil {
				continue
			}
			for _, p := range cand.Content.Parts {
				// Skip "thought" parts (Gemini 2.5); same rules as genai.GenerateContentResponse.Text().
				if p.Text == "" || p.Thought {
					continue
				}
				_ = l.Reenter(ctx, l.name, &frames.LLMTextFrame{Text: p.Text})
				textChunks++
			}
		}
	}
	if textChunks == 0 {
		log.Printf("%s: no text tokens streamed (safety filter, thinking-only output, or API issue) — TTS will not run", l.name)
	}
	_ = l.Reenter(ctx, l.name, &frames.LLMFullResponseEndFrame{})
	log.Printf("%s: stream finished (%d text chunks)", l.name, textChunks)
}
