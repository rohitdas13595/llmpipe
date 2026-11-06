package google

import (
	"context"
	"fmt"
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

	services.Slot
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewLLM(name, model string, client *genai.Client, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if model == "" {
		model = "gemini-2.0-flash"
	}
	return &LLM{name: name, Client: client, Model: model, Ctx: c, Reenter: re}
}

func (l *LLM) Name() string { return l.name }

func (l *LLM) push(ctx context.Context, f frames.Frame) error {
	return services.PushDown(ctx, l.H(), l.Reenter, l.name, f)
}

func (l *LLM) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch f.(type) {
	case *frames.InterruptionFrame, *frames.CancelFrame:
		l.mu.Lock()
		if l.cancel != nil {
			l.cancel()
			l.cancel = nil
		}
		l.mu.Unlock()
		emit.Down(f)
	case *frames.LLMRunFrame:
		if l.Client == nil || (l.H() == nil && l.Reenter == nil) {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("google llm: client or pipeline handle nil")})
			return nil
		}
		services.PipelineLog("llm", "%s: LLMRunFrame → Gemini stream model=%q", l.name, l.Model)
		go l.stream(l.TurnContext())
	default:
		emit.Down(f)
	}
	return nil
}

func (l *LLM) stream(parent context.Context) {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.mu.Unlock()
	defer cancel()

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

	services.PipelineLog("llm", "%s: GenerateContentStream model=%q contents=%d", l.name, l.Model, len(contents))
	_ = l.push(ctx, &frames.LLMFullResponseStartFrame{})
	stream := l.Client.Models.GenerateContentStream(ctx, l.Model, contents, cfg)
	textChunks := 0
	for resp, err := range stream {
		if ctx.Err() != nil {
			services.PipelineLog("llm", "%s: stream cancelled (%v)", l.name, ctx.Err())
			break
		}
		if err != nil {
			services.PipelineLog("llm", "%s: stream error: %v", l.name, err)
			_ = l.push(ctx, &frames.ErrorFrame{Err: err})
			break
		}
		if len(resp.Candidates) == 0 {
			if resp.PromptFeedback != nil {
				services.PipelineLog("llm", "%s: no candidates (prompt_feedback=%+v)", l.name, resp.PromptFeedback)
			}
			continue
		}
		for _, cand := range resp.Candidates {
			if cand.FinishReason != "" && cand.FinishReason != genai.FinishReasonStop {
				services.PipelineLog("llm", "%s: candidate finish_reason=%s", l.name, cand.FinishReason)
			}
			if cand.Content == nil {
				continue
			}
			for _, p := range cand.Content.Parts {
				// Skip "thought" parts (Gemini 2.5); same rules as genai.GenerateContentResponse.Text().
				if p.Text == "" || p.Thought {
					continue
				}
				_ = l.push(ctx, &frames.LLMTextFrame{Text: p.Text})
				textChunks++
			}
		}
	}
	if textChunks == 0 {
		services.PipelineLog("llm", "%s: no text tokens streamed (safety filter, thinking-only output, or API issue) — TTS will not run", l.name)
	}
	_ = l.push(ctx, &frames.LLMFullResponseEndFrame{})
	services.PipelineLog("llm", "%s: stream finished (%d text chunks)", l.name, textChunks)
}
