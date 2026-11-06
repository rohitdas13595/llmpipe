// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/audio/vad/vad_analyzer.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

package vad

import (
	"encoding/binary"
	"math"

	"github.com/rohitdas13595/llmpipe/internal/resample"
)

// State is the VAD state machine (Pipecat VADState).
type State int

const (
	Quiet State = iota
	Starting
	Speaking
	Stopping
)

func (s State) String() string {
	switch s {
	case Quiet:
		return "quiet"
	case Starting:
		return "starting"
	case Speaking:
		return "speaking"
	case Stopping:
		return "stopping"
	default:
		return "unknown"
	}
}

// Params are the confidence, timing, and volume gates.
// Zero Confidence / StartSecs / StopSecs are used as given: a zero start
// confirms speech on the first qualifying window. Use DefaultParams for the
// Pipecat defaults (0.7, 0.2s, 0.2s, 0.6).
type Params struct {
	Confidence float32
	StartSecs  float64
	StopSecs   float64
	MinVolume  float64
}

// DefaultParams matches Pipecat VAD_CONFIDENCE / START_SECS / STOP_SECS / MIN_VOLUME.
func DefaultParams() Params {
	return Params{Confidence: 0.7, StartSecs: 0.2, StopSecs: 0.2, MinVolume: 0.6}
}

// Model scores one analysis window. Samples are mono float32 in [-1, 1].
type Model interface {
	SampleRate() int
	WindowSamples() int
	Confidence(samples []float32) (float32, error)
	Reset()
}

// WindowNote is implemented by models that reset recurrent state during quiet
// audio (Silero). state is the machine state before this window is applied.
type WindowNote interface {
	NoteWindow(confidence float32, state State)
}

// StateAnalyzer runs a Model through Quiet → Starting → Speaking → Stopping.
// Analyze reports whether this buffer contained a transition into Speaking
// (started) and/or back to Quiet (stopped). A short utterance that begins and
// ends inside one buffer sets both.
type StateAnalyzer struct {
	model Model
	p     Params

	state         State
	startFrames   int
	stopFrames    int
	startingCount int
	stoppingCount int
	prevVolume    float64
	buf           []byte
	window        []float32
}

// NewStateAnalyzer builds an analyzer. m must return a positive sample rate
// and window size.
func NewStateAnalyzer(m Model, p Params) *StateAnalyzer {
	a := &StateAnalyzer{model: m, p: p, window: make([]float32, m.WindowSamples())}
	a.recompute()
	return a
}

// State is the current machine state.
func (a *StateAnalyzer) State() State { return a.state }

// Params returns the gates the analyzer is using.
func (a *StateAnalyzer) Params() Params { return a.p }

// SetParams replaces the gates and recomputes the frame counts. The volume
// smoother is left alone; the speech state returns to Quiet.
func (a *StateAnalyzer) SetParams(p Params) {
	a.p = p
	a.recompute()
	a.state = Quiet
	a.startingCount = 0
	a.stoppingCount = 0
}

// Reset clears the machine and the model.
func (a *StateAnalyzer) Reset() {
	a.state = Quiet
	a.startingCount = 0
	a.stoppingCount = 0
	a.prevVolume = 0
	a.buf = a.buf[:0]
	if a.model != nil {
		a.model.Reset()
	}
}

func (a *StateAnalyzer) recompute() {
	win := a.model.WindowSamples()
	sr := a.model.SampleRate()
	if win < 1 {
		win = 1
	}
	if sr < 1 {
		sr = 16000
	}
	frameSec := float64(win) / float64(sr)
	a.startFrames = int(math.Round(a.p.StartSecs / frameSec))
	a.stopFrames = int(math.Round(a.p.StopSecs / frameSec))
	if cap(a.window) < win {
		a.window = make([]float32, win)
	} else {
		a.window = a.window[:win]
	}
}

// Analyze consumes mono s16le PCM at sr and returns whether speech started
// and/or stopped while draining whole model windows. Leftover samples stay
// buffered. sr <= 0 is treated as the model rate.
func (a *StateAnalyzer) Analyze(pcm []byte, sr int) (started, stopped bool) {
	if a.model == nil || len(pcm) < 2 {
		return false, false
	}
	modelSR := a.model.SampleRate()
	if modelSR <= 0 {
		modelSR = 16000
	}
	if sr <= 0 {
		sr = modelSR
	}
	if sr != modelSR {
		pcm = resample.LinearS16LE(pcm, sr, modelSR)
	}
	a.buf = append(a.buf, pcm...)
	winBytes := a.model.WindowSamples() * 2
	if winBytes < 2 {
		return false, false
	}
	for len(a.buf) >= winBytes {
		frame := a.buf[:winBytes]
		a.buf = a.buf[winBytes:]
		n := winBytes / 2
		w := a.window[:n]
		for i := 0; i < n; i++ {
			s := int16(binary.LittleEndian.Uint16(frame[i*2:]))
			w[i] = float32(s) / 32768
		}
		conf, err := a.model.Confidence(w)
		if err != nil {
			conf = 0
		}
		vol := SmoothVolume(a.prevVolume, WindowVolume(frame), VolumeSmooth)
		a.prevVolume = vol
		prev := a.state
		a.advance(conf, vol)
		// A failed start (Starting → Quiet) is not an end of speech, and a
		// resume (Stopping → Speaking) is not a new start.
		if a.state == Speaking && (prev == Quiet || prev == Starting) {
			started = true
		}
		if a.state == Quiet && (prev == Speaking || prev == Stopping) {
			stopped = true
		}
		if note, ok := a.model.(WindowNote); ok {
			note.NoteWindow(conf, prev)
		}
	}
	// buf may alias the consumed prefix; compact so the next append is safe.
	if len(a.buf) > 0 {
		kept := make([]byte, len(a.buf))
		copy(kept, a.buf)
		a.buf = kept
	} else {
		a.buf = a.buf[:0]
	}
	return started, stopped
}

// advance applies one window.
func (a *StateAnalyzer) advance(conf float32, volume float64) {
	speaking := conf >= a.p.Confidence && volume >= a.p.MinVolume
	if speaking {
		switch a.state {
		case Quiet:
			a.state = Starting
			a.startingCount = 1
		case Starting:
			a.startingCount++
		case Stopping:
			a.state = Speaking
			a.stoppingCount = 0
		}
	} else {
		switch a.state {
		case Starting:
			a.state = Quiet
			a.startingCount = 0
		case Speaking:
			a.state = Stopping
			a.stoppingCount = 1
		case Stopping:
			a.stoppingCount++
		}
	}
	if a.state == Starting && a.startingCount >= a.startFrames {
		a.state = Speaking
		a.startingCount = 0
	}
	if a.state == Stopping && a.stoppingCount >= a.stopFrames {
		a.state = Quiet
		a.stoppingCount = 0
	}
}

// StartFrames is how many consecutive qualifying windows confirm speech.
// Exposed for tests of the Pipecat rounding (round(start_secs / frame_secs)).
func (a *StateAnalyzer) StartFrames() int { return a.startFrames }

// StopFrames is how many consecutive non-speech windows confirm silence.
func (a *StateAnalyzer) StopFrames() int { return a.stopFrames }
