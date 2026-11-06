package aggregate

import (
	"context"
	"strings"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// DefaultVADInterrupt is the barge-in confirmation used by turns.VADStart.
// The aggregator no longer applies it; the constant remains so callers that
// read it keep compiling.
const DefaultVADInterrupt = 300 * time.Millisecond

// UserAggregatorParams controls how finals in one turn become a single LLM run.
type UserAggregatorParams struct {
	// AggregationTimeout is how long to wait after the user stops, or after
	// the latest final transcript, before one LLMRunFrame is emitted.
	// Zero emits immediately. The constructor default is 500ms; call SetParams
	// to change it.
	AggregationTimeout time.Duration
}

// DefaultAggregationTimeout waits for a trailing final from streaming STT
// before the turn is closed.
const DefaultAggregationTimeout = 500 * time.Millisecond

// UserAggregator appends transcriptions and triggers one LLM run per turn.
// Finals that arrive while the user is still speaking are buffered. After the
// user stops, the buffer is held for AggregationTimeout so a late final can
// join the same user message. Barge-in is decided by turns.UserTurnProcessor,
// which pushes InterruptionTaskFrame; this aggregator does not.
type UserAggregator struct {
	name string
	ctx  *LLMContext
	bot  *BotState
	// InterruptStrategy is accepted for compatibility and ignored.
	InterruptStrategy interrupt.Strategy
	// VADInterrupt is accepted for compatibility and ignored.
	// turns.VADStart owns the barge-in confirmation.
	VADInterrupt time.Duration

	services.Slot

	timeout    time.Duration
	userSpeech bool
	gen        int
	buf        strings.Builder
}

// turnReady is queued back onto this aggregator when the aggregation timer fires.
type turnReady struct {
	gen int
	frames.System
	frames.Meta
}

func (turnReady) FrameKind() string { return "UserTurnReady" }

func NewUserAggregator(name string, c *LLMContext, bot *BotState, strategy interrupt.Strategy) *UserAggregator {
	if strategy == nil {
		strategy = interrupt.MinWords{N: 1}
	}
	return &UserAggregator{
		name:              name,
		ctx:               c,
		bot:               bot,
		InterruptStrategy: strategy,
		timeout:           DefaultAggregationTimeout,
	}
}

// SetParams replaces the aggregation timeout. Zero emits the run as soon as
// the turn is closed (or immediately, when a final arrives outside a turn).
func (u *UserAggregator) SetParams(p UserAggregatorParams) {
	u.timeout = p.AggregationTimeout
}

func (u *UserAggregator) Name() string { return u.name }

func (u *UserAggregator) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.BotStartedSpeakingFrame, *frames.BotSpeakingFrame:
		u.setSpeaking(true)
		processor.Forward(dir, f, emit)
	case *frames.BotStoppedSpeakingFrame:
		u.setSpeaking(false)
		processor.Forward(dir, f, emit)
	case *frames.UserStartedSpeakingFrame:
		if u.buf.Len() > 0 {
			u.emitRun(emit)
		}
		u.userSpeech = true
		processor.Forward(dir, f, emit)
	case *frames.UserStoppedSpeakingFrame:
		u.userSpeech = false
		processor.Forward(dir, f, emit)
		u.arm(emit)
	case *turnReady:
		if fr.gen != u.gen {
			return nil
		}
		u.emitRun(emit)
	case *frames.InterimTranscriptionFrame:
		processor.Forward(dir, f, emit)
	case *frames.TranscriptionFrame:
		if strings.TrimSpace(fr.Text) == "" {
			processor.Forward(dir, f, emit)
			return nil
		}
		u.append(fr.Text)
		if u.userSpeech {
			return nil
		}
		u.arm(emit)
	default:
		processor.Forward(dir, f, emit)
	}
	return nil
}

func (u *UserAggregator) setSpeaking(v bool) {
	if u.bot != nil {
		u.bot.SetSpeaking(v)
	}
	if !v {
		u.userSpeech = false
	}
}

func (u *UserAggregator) append(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	if u.buf.Len() > 0 {
		u.buf.WriteByte(' ')
	}
	u.buf.WriteString(text)
}

// arm starts the aggregation timer. A timeout of zero, or a processor that
// is not inside a task, emits the run on this call.
func (u *UserAggregator) arm(emit processor.Emit) {
	if strings.TrimSpace(u.buf.String()) == "" {
		return
	}
	if u.timeout <= 0 || u.H() == nil {
		u.emitRun(emit)
		return
	}
	u.gen++
	gen := u.gen
	h := u.H()
	d := u.timeout
	time.AfterFunc(d, func() {
		_ = h.QueueSelf(&turnReady{gen: gen})
	})
}

func (u *UserAggregator) emitRun(emit processor.Emit) {
	u.gen++
	text := strings.TrimSpace(u.buf.String())
	u.buf.Reset()
	if text == "" {
		return
	}
	if u.ctx != nil {
		u.ctx.AppendUser(text)
	}
	services.PipelineLog("pipeline", "%s: transcript OK → LLM run (%q)", u.name, text)
	if emit.Down != nil {
		emit.Down(&frames.LLMRunFrame{})
	}
}
