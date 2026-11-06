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

// UserAggregator appends transcriptions and triggers LLM runs. While the bot
// is speaking it asks the source for one pipeline interruption, either from
// the word strategy (interim or final) or after DefaultVADInterrupt of audio.
type UserAggregator struct {
	name     string
	ctx      *LLMContext
	bot      *BotState
	strategy interrupt.Strategy
	// VADInterrupt is the speech duration that counts as a barge-in when no
	// transcript has arrived. Zero uses DefaultVADInterrupt. Negative disables
	// the VAD path. NoInterrupt disables both paths.
	VADInterrupt time.Duration

	userSpeech  bool
	speechFor   time.Duration
	interrupted bool
	pendingRun  bool
}

func NewUserAggregator(name string, c *LLMContext, bot *BotState, strategy interrupt.Strategy) *UserAggregator {
	if strategy == nil {
		strategy = interrupt.MinWords{N: 1}
	}
	return &UserAggregator{name: name, ctx: c, bot: bot, strategy: strategy}
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
		u.userSpeech = true
		u.speechFor = 0
		processor.Forward(dir, f, emit)
	case *frames.UserStoppedSpeakingFrame, *frames.VADUserStoppedSpeakingFrame:
		u.userSpeech = false
		u.speechFor = 0
		processor.Forward(dir, f, emit)
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
		asked := u.maybeText(fr.Text, emit)
		u.ctx.AppendUser(fr.Text)
		services.PipelineLog("pipeline", "%s: transcript OK → LLM run (%q)", u.name, strings.TrimSpace(fr.Text))
		if asked {
			u.pendingRun = true
			return nil
		}
		if emit.Down != nil {
			emit.Down(&frames.LLMRunFrame{})
		}
	case *frames.InterruptionFrame:
		processor.Forward(dir, f, emit)
		if u.pendingRun && dir != processor.Upstream && emit.Down != nil {
			u.pendingRun = false
			emit.Down(&frames.LLMRunFrame{})
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

func (u *UserAggregator) ask(emit processor.Emit) {
	if u.interrupted {
		return
	}
	u.interrupted = true
	if emit.Up != nil {
		emit.Up(&frames.InterruptionTaskFrame{})
	}
}
