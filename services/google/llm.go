package google

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"sync"

	"google.golang.org/genai"

	gem "github.com/rohitdas13595/llmpipe/adapters/gemini"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/llmbase"
)

// LLM uses Google GenAI (Gemini) streaming.
type LLM struct {
	name     string
	Client   *genai.Client
	Model    string
	Ctx      *aggregate.LLMContext
	Reenter  services.ReenterFunc
	tools    *llmbase.Bag
	generate func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) iter.Seq2[*genai.GenerateContentResponse, error]

	services.Slot
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewLLM(name, model string, client *genai.Client, c *aggregate.LLMContext, re services.ReenterFunc) *LLM {
	if model == "" {
		model = "gemini-2.0-flash"
	}
	return &LLM{name: name, Client: client, Model: model, Ctx: c, Reenter: re, tools: &llmbase.Bag{}}
}

func (l *LLM) Name() string { return l.name }

// SetRegistry installs function handlers for Gemini function calls.
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
		if (l.Client == nil && l.generate == nil) || (l.H() == nil && l.Reenter == nil) {
			emit.Down(&frames.ErrorFrame{Err: fmt.Errorf("google llm: client or pipeline handle nil")})
			return nil
		}
		services.PipelineLog("llm", "%s: LLMRunFrame → Gemini stream model=%q", l.name, l.Model)
		go l.stream(l.TurnContext())
		return nil
	}
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
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

	raw, err := gem.API{}.Messages(l.Ctx.Messages())
	if err != nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: err})
		return
	}
	prompt, ok := raw.(gem.Prompt)
	if !ok {
		_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("google llm: bad prompt")})
		return
	}
	cfg := &genai.GenerateContentConfig{}
	if prompt.System != nil {
		cfg.SystemInstruction = prompt.System
	}
	schema := l.tools.Schema()
	if len(schema.Functions) > 0 {
		toolRaw, err := gem.API{}.Tools(schema)
		if err != nil {
			_ = l.push(ctx, &frames.ErrorFrame{Err: err})
			return
		}
		declared, ok := toolRaw.([]*genai.Tool)
		if !ok {
			_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("google llm: bad tools")})
			return
		}
		cfg.Tools = declared
	}

	seq := l.generate
	if seq == nil && l.Client != nil {
		seq = func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) iter.Seq2[*genai.GenerateContentResponse, error] {
			return l.Client.Models.GenerateContentStream(ctx, model, contents, cfg)
		}
	}
	if seq == nil {
		_ = l.push(ctx, &frames.ErrorFrame{Err: fmt.Errorf("google llm: no client")})
		return
	}

	services.PipelineLog("llm", "%s: GenerateContentStream model=%q contents=%d", l.name, l.Model, len(prompt.Contents))
	_ = l.push(ctx, &frames.LLMFullResponseStartFrame{})
	var spoken strings.Builder
	order := []string{}
	byID := map[string]*aggregate.ToolCall{}
	textChunks := 0
	for resp, err := range seq(ctx, l.Model, prompt.Contents, cfg) {
		if ctx.Err() != nil {
			services.PipelineLog("llm", "%s: stream cancelled (%v)", l.name, ctx.Err())
			break
		}
		if err != nil {
			services.PipelineLog("llm", "%s: stream error: %v", l.name, err)
			_ = l.push(ctx, &frames.ErrorFrame{Err: err})
			break
		}
		if resp == nil || len(resp.Candidates) == 0 {
			if resp != nil && resp.PromptFeedback != nil {
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
				if p == nil {
					continue
				}
				if p.FunctionCall != nil {
					collectCall(byID, &order, p.FunctionCall)
				}
				if p.Text == "" || p.Thought {
					continue
				}
				spoken.WriteString(p.Text)
				_ = l.push(ctx, &frames.LLMTextFrame{Text: p.Text})
				textChunks++
			}
		}
	}
	calls := orderedCalls(byID, order)
	if len(calls) == 0 || ctx.Err() != nil {
		if textChunks == 0 && len(calls) == 0 {
			services.PipelineLog("llm", "%s: no text tokens streamed (safety filter, thinking-only output, or API issue) — TTS will not run", l.name)
		}
		_ = l.push(context.Background(), &frames.LLMFullResponseEndFrame{})
		services.PipelineLog("llm", "%s: stream finished (%d text chunks)", l.name, textChunks)
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

func collectCall(byID map[string]*aggregate.ToolCall, order *[]string, fc *genai.FunctionCall) {
	if fc == nil {
		return
	}
	key := fc.ID
	if key == "" {
		key = fmt.Sprintf("%s#%d", fc.Name, len(*order))
	}
	tc := byID[key]
	if tc == nil {
		tc = &aggregate.ToolCall{ID: key, Name: fc.Name}
		byID[key] = tc
		*order = append(*order, key)
	}
	if fc.Name != "" {
		tc.Name = fc.Name
	}
	if fc.ID != "" {
		tc.ID = fc.ID
	}
	if len(fc.Args) > 0 {
		if b, err := json.Marshal(fc.Args); err == nil {
			tc.Args = b
		}
	}
}

func orderedCalls(byID map[string]*aggregate.ToolCall, order []string) []aggregate.ToolCall {
	out := make([]aggregate.ToolCall, 0, len(order))
	for _, id := range order {
		tc := *byID[id]
		if len(tc.Args) == 0 {
			tc.Args = json.RawMessage(`{}`)
		}
		out = append(out, tc)
	}
	return out
}
