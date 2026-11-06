package aggregate

import (
	"context"
	"strings"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// AssistantAggregator commits assistant text that was actually played.
// Place it after the output. TTSTextFrame values released by the output are
// the spoken words. When the pipeline has no TTS, the full LLM text is
// committed at the end of the response.
type AssistantAggregator struct {
	name    string
	ctx     *LLMContext
	llm     strings.Builder
	spoken  strings.Builder
	sawTTS  bool
	ended   bool
	stopped bool
	closed  bool
}

func NewAssistantAggregator(name string, c *LLMContext) *AssistantAggregator {
	return &AssistantAggregator{name: name, ctx: c}
}

func (a *AssistantAggregator) Name() string { return a.name }

func (a *AssistantAggregator) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.LLMFullResponseStartFrame:
		// Played TTS text is system priority, so it can reach this aggregator
		// before the control frame that opened the response. Keep that text.
		if a.closed || !a.sawTTS {
			a.reset()
		}
		processor.Forward(dir, f, emit)
	case *frames.LLMTextFrame:
		a.llm.WriteString(fr.Text)
		if emit.Down != nil && dir != processor.Upstream {
			emit.Down(&frames.TextFrame{Text: fr.Text})
		}
	case *frames.TTSStartedFrame:
		a.sawTTS = true
		processor.Forward(dir, f, emit)
	case *frames.TTSTextFrame:
		a.sawTTS = true
		appendSpoken(&a.spoken, fr.Text)
		processor.Forward(dir, f, emit)
	case *frames.TTSStoppedFrame:
		a.sawTTS = true
		processor.Forward(dir, f, emit)
	case *frames.LLMFullResponseEndFrame:
		a.ended = true
		a.maybeCommit()
		processor.Forward(dir, f, emit)
	case *frames.BotStoppedSpeakingFrame:
		a.stopped = true
		if fr.Interrupted {
			a.commitPlayed()
		} else {
			a.maybeCommit()
		}
		processor.Forward(dir, f, emit)
	case *frames.InterruptionFrame:
		a.commitPlayed()
		processor.Forward(dir, f, emit)
	default:
		processor.Forward(dir, f, emit)
	}
	return nil
}

func (a *AssistantAggregator) maybeCommit() {
	if a.closed {
		return
	}
	if a.sawTTS {
		if a.ended && a.stopped {
			a.commit(a.spoken.String())
		}
		return
	}
	if a.ended {
		a.commit(a.llm.String())
	}
}

func (a *AssistantAggregator) commitPlayed() {
	if a.closed {
		return
	}
	if a.sawTTS {
		a.commit(a.spoken.String())
		return
	}
	// Nothing was synthesized, so nothing was heard.
	a.closed = true
	a.llm.Reset()
	a.spoken.Reset()
}

func (a *AssistantAggregator) commit(text string) {
	if a.closed {
		return
	}
	a.closed = true
	text = strings.TrimSpace(text)
	if text != "" && a.ctx != nil {
		a.ctx.AppendAssistant(text)
	}
	a.llm.Reset()
	a.spoken.Reset()
}

func (a *AssistantAggregator) reset() {
	a.llm.Reset()
	a.spoken.Reset()
	a.sawTTS = false
	a.ended = false
	a.stopped = false
	a.closed = false
}

func appendSpoken(b *strings.Builder, text string) {
	if text == "" {
		return
	}
	if b.Len() > 0 {
		prev := b.String()
		if !strings.HasSuffix(prev, " ") && !strings.HasPrefix(text, " ") {
			b.WriteByte(' ')
		}
	}
	b.WriteString(text)
}
