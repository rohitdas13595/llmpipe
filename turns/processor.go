package turns

import (
	"context"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// Scheduler is implemented by stop strategies that need a timer. The frame
// is queued back onto this processor when the duration elapses.
type Scheduler interface {
	Arm() (after time.Duration, frame frames.Frame, ok bool)
}

// AnalyzerLauncher is implemented by stop strategies that run a model off
// the processor goroutine. The processor calls AnalyzeEndOfTurn and queues
// the result with QueueSelf.
type AnalyzerLauncher interface {
	Pending() (turn.Analyzer, int, bool)
}

// Ender is told when the user turn has been closed.
type Ender interface {
	OnTurnEnded()
}

// Feeder receives every input-audio frame, including audio from before the
// user turn is confirmed, so an analyzer can keep a pre-speech ring.
type Feeder interface {
	Feed(f frames.Frame, s *TurnState) (stop bool)
}

// UserTurnProcessor turns VAD and transcript frames into one user turn.
// It emits UserStartedSpeaking and UserStoppedSpeaking downstream, pushes
// InterruptionTaskFrame upstream when a turn starts while the bot is
// speaking, and drops input audio while a mute strategy says so.
type UserTurnProcessor struct {
	services.Slot
	name  string
	start []StartStrategy
	stop  []StopStrategy
	mute  []MuteStrategy
	state TurnState
}

// NewUserTurnProcessor builds the processor. Nil slices are fine.
func NewUserTurnProcessor(name string, start []StartStrategy, stop []StopStrategy, mute []MuteStrategy) *UserTurnProcessor {
	return &UserTurnProcessor{name: name, start: start, stop: stop, mute: mute}
}

func (p *UserTurnProcessor) Name() string { return p.name }

func (p *UserTurnProcessor) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	p.note(f)
	if dir == processor.Upstream {
		p.audioMuted(f)
		processor.Forward(dir, f, emit)
		return nil
	}
	if isInternal(f) {
		p.applyStop(ctx, f, emit)
		p.schedule()
		return nil
	}
	muted := p.audioMuted(f)
	if !p.state.UserSpeaking && !muted {
		for _, s := range p.start {
			if s.ShouldStart(f, &p.state) {
				p.state.UserSpeaking = true
				if emit.Down != nil {
					emit.Down(&frames.UserStartedSpeakingFrame{})
				}
				if p.state.BotSpeaking && emit.Up != nil {
					emit.Up(&frames.InterruptionTaskFrame{})
				}
				break
			}
		}
	}
	if muted && isUserAudio(f) {
		p.schedule()
		return nil
	}
	if emit.Down != nil {
		emit.Down(f)
	}
	if isUserAudio(f) && p.feed(f) && p.state.UserSpeaking {
		p.finish(emit)
		p.schedule()
		return nil
	}
	if p.state.UserSpeaking {
		p.applyStop(ctx, f, emit)
	}
	p.schedule()
	return nil
}

func (p *UserTurnProcessor) note(f frames.Frame) {
	switch fr := f.(type) {
	case *frames.VADUserStartedSpeakingFrame:
		p.state.VADSpeaking = true
		p.state.Speech = 0
	case *frames.VADUserStoppedSpeakingFrame:
		p.state.VADSpeaking = false
	case *frames.BotStartedSpeakingFrame, *frames.BotSpeakingFrame:
		p.state.BotSpeaking = true
	case *frames.BotStoppedSpeakingFrame:
		p.state.BotSpeaking = false
	case *frames.TranscriptionFrame:
		if text := frameText(fr); text != "" {
			p.state.HasTranscript = true
			p.state.Transcript = text
		}
	case *frames.InterimTranscriptionFrame:
		if text := frameText(fr); text != "" {
			p.state.HasTranscript = true
			p.state.Transcript = text
		}
	}
}

func (p *UserTurnProcessor) audioMuted(f frames.Frame) bool {
	muted := false
	for _, m := range p.mute {
		if m.Muted(f, &p.state) {
			muted = true
		}
	}
	return muted
}

func (p *UserTurnProcessor) applyStop(ctx context.Context, f frames.Frame, emit processor.Emit) {
	for _, s := range p.stop {
		decided, stop := s.ShouldStop(ctx, f, &p.state)
		if !decided || !stop {
			continue
		}
		p.finish(emit)
		return
	}
}

func (p *UserTurnProcessor) finish(emit processor.Emit) {
	p.state.UserSpeaking = false
	p.state.Speech = 0
	p.state.HasTranscript = false
	p.state.Transcript = ""
	if emit.Down != nil {
		emit.Down(&frames.UserStoppedSpeakingFrame{})
	}
	for _, s := range p.stop {
		if e, ok := s.(Ender); ok {
			e.OnTurnEnded()
		}
	}
}

func (p *UserTurnProcessor) feed(f frames.Frame) bool {
	stop := false
	for _, s := range p.stop {
		fd, ok := s.(Feeder)
		if !ok {
			continue
		}
		if fd.Feed(f, &p.state) {
			stop = true
		}
	}
	return stop
}

func (p *UserTurnProcessor) schedule() {
	h := p.H()
	for _, s := range p.stop {
		if sc, ok := s.(Scheduler); ok {
			if d, frame, ok := sc.Arm(); ok && h != nil && frame != nil {
				time.AfterFunc(d, func() { _ = h.QueueSelf(frame) })
			}
		}
		if launcher, ok := s.(AnalyzerLauncher); ok {
			an, gen, ready := launcher.Pending()
			if !ready || h == nil || an == nil {
				continue
			}
			go func(an turn.Analyzer, gen int) {
				res, prob, err := an.AnalyzeEndOfTurn(h.Context())
				_ = h.QueueSelf(&analysisResult{gen: gen, result: res, prob: prob, err: err})
			}(an, gen)
		}
	}
}

func isUserAudio(f frames.Frame) bool {
	_, ok := f.(*frames.InputAudioRawFrame)
	return ok
}

func isInternal(f frames.Frame) bool {
	switch f.(type) {
	case *speechFire, *analysisResult:
		return true
	default:
		return false
	}
}
