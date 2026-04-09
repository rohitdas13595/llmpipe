package aggregate

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// AssistantAggregator forwards LLM tokens to TTS and commits assistant text to context.
type AssistantAggregator struct {
	name       string
	ctx        *LLMContext
	partialBuf string
}

func NewAssistantAggregator(name string, c *LLMContext) *AssistantAggregator {
	return &AssistantAggregator{name: name, ctx: c}
}

func (a *AssistantAggregator) Name() string { return a.name }

func (a *AssistantAggregator) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.LLMFullResponseStartFrame:
		a.partialBuf = ""
		emit.Down(f)
	case *frames.LLMTextFrame:
		a.partialBuf += fr.Text
		emit.Down(&frames.TextFrame{Text: fr.Text})
	case *frames.LLMFullResponseEndFrame:
		if a.partialBuf != "" {
			a.ctx.AppendAssistant(a.partialBuf)
		}
		a.partialBuf = ""
		emit.Down(f)
	case *frames.InterruptionFrame:
		a.partialBuf = ""
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}
