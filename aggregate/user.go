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

// DefaultVADInterrupt is how long user speech must last, while the bot is
// speaking, before a barge-in is requested without a transcript.
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
// join the same user message. While the bot is speaking it asks the source
// for one pipeline interruption, either from the word strategy (interim or
// final) or after DefaultVADInterrupt of audio.
type UserAggregator struct {
	name     string
	ctx      *LLMContext
	bot      *BotState
	strategy interrupt.Strategy
	// VADInterrupt is the speech duration that counts as a barge-in when no
	// transcript has arrived. Zero uses DefaultVADInterrupt. Negative disables
	// the VAD path. NoInterrupt disables both paths.
	VADInterrupt time.Duration

	services.Slot

	timeout     time.Duration
	userSpeech  bool
	speechFor   time.Duration
	interrupted bool
	pendingRun  bool
	gen         int
	buf         strings.Builder
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
		name:     name,
		ctx:      c,
		bot:      bot,
		strategy: strategy,
		timeout:  DefaultAggregationTimeout,
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
	case *frames.UserStartedSpeakingFrame, *frames.VADUserStartedSpeakingFrame:
		if u.buf.Len() > 0 {
			u.emitRun(emit)
		}
		u.userSpeech = true
		u.speechFor = 0
		processor.Forward(dir, f, emit)
	case *frames.UserStoppedSpeakingFrame, *frames.VADUserStoppedSpeakingFrame:
		u.userSpeech = false
		u.speechFor = 0
		processor.Forward(dir, f, emit)
		u.arm(emit)
	case *turnReady:
		if fr.gen != u.gen {
			return nil
		}
		u.emitRun(emit)
	case *frames.InputAudioRawFrame:
		u.noteAudio(fr, emit)
		processor.Forward(dir, f, emit)
	case *frames.InterimTranscriptionFrame:
		u.maybeText(fr.Text, emit)
		processor.Forward(dir, f, emit)
	case *frames.TranscriptionFrame:
		if strings.TrimSpace(fr.Text) == "" {
			processor.Forward(dir, f, emit)
			return nil
		}
		u.append(fr.Text)
		if u.maybeText(fr.Text, emit) {
			u.pendingRun = true
			return nil
		}
		if u.userSpeech {
			return nil
		}
		u.arm(emit)
	case *frames.InterruptionFrame:
		processor.Forward(dir, f, emit)
		if u.pendingRun && dir != processor.Upstream {
			u.pendingRun = false
			u.emitRun(emit)
		}
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
		u.interrupted = false
		u.userSpeech = false
		u.speechFor = 0
	}
}

func (u *UserAggregator) allow() bool {
	_, no := u.strategy.(interrupt.NoInterrupt)
	return !no
}

func (u *UserAggregator) vadGate() time.Duration {
	if u.VADInterrupt < 0 {
		return -1
	}
	if u.VADInterrupt == 0 {
		return DefaultVADInterrupt
	}
	return u.VADInterrupt
}

func (u *UserAggregator) maybeText(text string, emit processor.Emit) bool {
	if !u.allow() || u.interrupted || u.bot == nil || !u.bot.Speaking() {
		return false
	}
	if !u.strategy.ShouldInterrupt(text, true) {
		return false
	}
	u.ask(emit)
	return true
}

func (u *UserAggregator) noteAudio(fr *frames.InputAudioRawFrame, emit processor.Emit) {
	if !u.userSpeech || !u.allow() || u.interrupted || u.bot == nil || !u.bot.Speaking() {
		return
	}
	gate := u.vadGate()
	if gate < 0 {
		return
	}
	sr := fr.SampleRate
	if sr <= 0 {
		sr = 16000
	}
	if len(fr.Audio) < 2 {
		return
	}
	u.speechFor += time.Duration(len(fr.Audio)/2) * time.Second / time.Duration(sr)
	if u.speechFor >= gate {
		u.ask(emit)
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

func (u *UserAggregator) ask(emit processor.Emit) {
	if u.interrupted {
		return
	}
	u.interrupted = true
	if emit.Up != nil {
		emit.Up(&frames.InterruptionTaskFrame{})
	}
}
