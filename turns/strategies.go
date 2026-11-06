// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/turns/user_start, user_stop, and user_mute.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

// Package turns decides when a user turn starts, stops, and when the mic is muted.
package turns

import (
	"context"
	"strings"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/frames"
)

// TurnState is the snapshot strategies read and update.
type TurnState struct {
	UserSpeaking  bool
	BotSpeaking   bool
	VADSpeaking   bool
	Speech        time.Duration
	HasTranscript bool
	Transcript    string
}

// StartStrategy reports whether this frame begins a user turn.
type StartStrategy interface {
	ShouldStart(f frames.Frame, s *TurnState) bool
}

// StopStrategy reports whether this frame ends a user turn.
// decided is false when the strategy abstains. Any strategy that returns
// stop ends the turn.
type StopStrategy interface {
	ShouldStop(ctx context.Context, f frames.Frame, s *TurnState) (decided, stop bool)
}

// MuteStrategy reports whether user audio should be dropped.
// Implementations also update their own state from f.
type MuteStrategy interface {
	Muted(f frames.Frame, s *TurnState) bool
}

// VADStart begins a turn on VADUserStartedSpeaking.
// MinSpeechMS delays that start until this much further audio has arrived,
// and only while the bot is speaking. A quiet bot starts on the VAD frame
// itself, so ordinary turns are not held for the barge-in confirmation.
// MinSpeechMS 0 always starts on the VAD frame.
type VADStart struct {
	MinSpeechMS int
}

func (v VADStart) ShouldStart(f frames.Frame, s *TurnState) bool {
	switch fr := f.(type) {
	case *frames.VADUserStartedSpeakingFrame:
		if v.MinSpeechMS <= 0 || !s.BotSpeaking {
			return true
		}
		return false
	case *frames.InputAudioRawFrame:
		if !s.VADSpeaking || v.MinSpeechMS <= 0 || !s.BotSpeaking {
			return false
		}
		s.Speech += pcmDuration(fr)
		return s.Speech >= time.Duration(v.MinSpeechMS)*time.Millisecond
	default:
		return false
	}
}

// TranscriptionStart begins a turn on any non-empty interim or final.
type TranscriptionStart struct{}

func (TranscriptionStart) ShouldStart(f frames.Frame, s *TurnState) bool {
	return frameText(f) != ""
}

// MinWordsStart begins a turn once a transcript contains at least N words.
// WhenBotSpeaking limits that to barge-in. N <= 0 means 1.
type MinWordsStart struct {
	N               int
	WhenBotSpeaking bool
}

func (m MinWordsStart) ShouldStart(f frames.Frame, s *TurnState) bool {
	text := frameText(f)
	if text == "" {
		return false
	}
	if m.WhenBotSpeaking && !s.BotSpeaking {
		return false
	}
	n := m.N
	if n <= 0 {
		n = 1
	}
	return len(strings.Fields(text)) >= n
}

// SpeechTimeoutStop ends the turn after Timeout of not-speaking following a
// VAD stop. Timeout 0 ends on the VAD-stopped frame itself. A positive
// timeout is also armed when a transcript arrives while the VAD is quiet,
// so a text-only turn still closes. RequireTranscript waits for some text
// before the stop is allowed.
type SpeechTimeoutStop struct {
	Timeout           time.Duration
	RequireTranscript bool

	gen       int
	armed     bool
	timerDone bool
}

// NewSpeechTimeoutStop returns a strategy. A non-positive timeout becomes 600ms,
// Pipecat's user_speech_timeout default. Construct the struct directly to stop
// on the VAD frame with no wait.
func NewSpeechTimeoutStop(timeout time.Duration) *SpeechTimeoutStop {
	if timeout <= 0 {
		timeout = 600 * time.Millisecond
	}
	return &SpeechTimeoutStop{Timeout: timeout}
}

func (s *SpeechTimeoutStop) ShouldStop(ctx context.Context, f frames.Frame, st *TurnState) (bool, bool) {
	switch fr := f.(type) {
	case *frames.VADUserStartedSpeakingFrame:
		s.gen++
		s.armed = false
		s.timerDone = false
		return false, false
	case *frames.VADUserStoppedSpeakingFrame:
		s.gen++
		s.timerDone = false
		if s.Timeout <= 0 {
			if s.RequireTranscript && !st.HasTranscript {
				s.timerDone = true
				return false, false
			}
			return true, true
		}
		s.armed = true
		return false, false
	case *speechFire:
		if fr.gen != s.gen {
			return false, false
		}
		if s.RequireTranscript && !st.HasTranscript {
			s.timerDone = true
			return false, false
		}
		return true, true
	case *frames.TranscriptionFrame, *frames.InterimTranscriptionFrame:
		if st.VADSpeaking {
			return false, false
		}
		if s.timerDone && st.HasTranscript {
			return true, true
		}
		if s.Timeout > 0 && !s.armed {
			s.gen++
			s.armed = true
		}
		return false, false
	default:
		return false, false
	}
}

// Arm returns a timer the processor should enqueue back onto itself.
func (s *SpeechTimeoutStop) Arm() (time.Duration, frames.Frame, bool) {
	if !s.armed || s.Timeout <= 0 {
		return 0, nil, false
	}
	s.armed = false
	return s.Timeout, &speechFire{gen: s.gen}, true
}

// OnTurnEnded cancels a pending timer.
func (s *SpeechTimeoutStop) OnTurnEnded() {
	s.gen++
	s.armed = false
	s.timerDone = false
}

type speechFire struct {
	gen int
	frames.System
	frames.Meta
}

func (speechFire) FrameKind() string { return "SpeechTimeout" }

// TurnAnalyzerStop asks a turn.Analyzer to judge the end of the turn when the
// VAD stops, and also honours the analyzer's own silence fallback from
// AppendAudio. The processor runs AnalyzeEndOfTurn on a worker and queues the
// result back with QueueSelf.
type TurnAnalyzerStop struct {
	Analyzer turn.Analyzer

	gen      int
	waiting  bool
	launched bool
}

func (t *TurnAnalyzerStop) ShouldStop(ctx context.Context, f frames.Frame, st *TurnState) (bool, bool) {
	switch fr := f.(type) {
	case *frames.VADUserStartedSpeakingFrame:
		t.gen++
		t.waiting = false
		t.launched = false
		return false, false
	case *frames.VADUserStoppedSpeakingFrame:
		t.gen++
		t.waiting = t.Analyzer != nil
		t.launched = false
		return false, false
	case *analysisResult:
		if !t.waiting || fr.gen != t.gen {
			return false, false
		}
		t.waiting = false
		if fr.err != nil {
			return false, false
		}
		return true, fr.result == turn.Complete
	default:
		return false, false
	}
}

// Feed appends audio so the pre-speech ring fills before the user turn is
// confirmed. A true result is the analyzer's silence fallback.
func (t *TurnAnalyzerStop) Feed(f frames.Frame, st *TurnState) bool {
	fr, ok := f.(*frames.InputAudioRawFrame)
	if !ok || t.Analyzer == nil {
		return false
	}
	return t.Analyzer.AppendAudio(fr.Audio, fr.SampleRate, st.VADSpeaking) == turn.Complete
}

// Pending reports an analysis the processor should start. It returns true once
// per VAD stop.
func (t *TurnAnalyzerStop) Pending() (turn.Analyzer, int, bool) {
	if t.Analyzer == nil || !t.waiting || t.launched {
		return nil, 0, false
	}
	t.launched = true
	return t.Analyzer, t.gen, true
}

// OnTurnEnded drops the analyzer buffer.
func (t *TurnAnalyzerStop) OnTurnEnded() {
	t.gen++
	t.waiting = false
	t.launched = false
	if t.Analyzer != nil {
		t.Analyzer.Clear()
	}
}

type analysisResult struct {
	gen    int
	result turn.Result
	prob   float32
	err    error
	frames.System
	frames.Meta
}

func (analysisResult) FrameKind() string { return "TurnAnalysis" }

// FirstSpeechMute drops user audio until the bot has finished its first utterance.
type FirstSpeechMute struct {
	passed bool
}

func (m *FirstSpeechMute) Muted(f frames.Frame, s *TurnState) bool {
	if _, ok := f.(*frames.BotStoppedSpeakingFrame); ok {
		m.passed = true
	}
	return !m.passed
}

// FunctionCallMute drops user audio while function calls are in flight.
// FunctionCallsStarted increments the count by Calls() when the frame
// reports a batch size, otherwise by one. FunctionCallResult and
// FunctionCallCancel each decrement it. The service pushes the started
// frame and a successful result upstream, and a cancel (not the result)
// upstream, so one call moves the count by one.
type FunctionCallMute struct {
	inflight int
}

func (m *FunctionCallMute) Muted(f frames.Frame, s *TurnState) bool {
	switch f.FrameKind() {
	case "FunctionCallsStarted":
		n := 1
		if c, ok := f.(interface{ Calls() int }); ok && c.Calls() > 0 {
			n = c.Calls()
		}
		m.inflight += n
	case frames.KindFunctionCallResult, "FunctionCallCancel":
		if m.inflight > 0 {
			m.inflight--
		}
	}
	return m.inflight > 0
}

func frameText(f frames.Frame) string {
	switch fr := f.(type) {
	case *frames.TranscriptionFrame:
		return strings.TrimSpace(fr.Text)
	case *frames.InterimTranscriptionFrame:
		return strings.TrimSpace(fr.Text)
	default:
		return ""
	}
}

func pcmDuration(f *frames.InputAudioRawFrame) time.Duration {
	sr := f.SampleRate
	if sr <= 0 {
		sr = 16000
	}
	n := len(f.Audio) / 2
	if n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second / time.Duration(sr)
}
