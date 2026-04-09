package aggregate

import (
	"context"
	"strings"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// UserAggregator appends transcriptions and triggers LLM runs.
type UserAggregator struct {
	name     string
	ctx      *LLMContext
	bot      *BotState
	strategy interrupt.Strategy
}

func NewUserAggregator(name string, c *LLMContext, bot *BotState, strategy interrupt.Strategy) *UserAggregator {
	if strategy == nil {
		strategy = interrupt.NoInterrupt{}
	}
	return &UserAggregator{name: name, ctx: c, bot: bot, strategy: strategy}
}

func (u *UserAggregator) Name() string { return u.name }

func (u *UserAggregator) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.InterimTranscriptionFrame:
		if u.bot.Speaking() && u.strategy.ShouldInterrupt(fr.Text, u.bot.Speaking()) {
			emit.Down(&frames.InterruptionFrame{})
		}
		emit.Down(f)
	case *frames.TranscriptionFrame:
		if strings.TrimSpace(fr.Text) == "" {
			emit.Down(f)
			return nil
		}
		if u.bot.Speaking() && u.strategy.ShouldInterrupt(fr.Text, u.bot.Speaking()) {
			emit.Down(&frames.InterruptionFrame{})
		}
		u.ctx.AppendUser(fr.Text)
		services.PipelineLog("pipeline", "%s: transcript OK → LLM run (%q)", u.name, strings.TrimSpace(fr.Text))
		emit.Down(&frames.LLMRunFrame{})
	default:
		emit.Down(f)
	}
	return nil
}
