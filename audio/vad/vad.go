// Package vad provides voice activity detection.
package vad

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// Analyzer analyzes audio chunks for speech (Silero can replace this implementation).
type Analyzer interface {
	Analyze(pcm16 []byte, sampleRate int) (started, stopped bool)
}

// EnergyAnalyzer is a simple RMS threshold VAD (MVP).
type EnergyAnalyzer struct {
	Threshold float64 // RMS threshold 0..32768
	MinSpeech int     // consecutive speech frames to start
	MinSilence int   // consecutive silence frames to stop
	speechRun int
	silenceRun int
	inSpeech   bool
}

func NewEnergyAnalyzer(threshold float64, minSpeech, minSilence int) *EnergyAnalyzer {
	if threshold <= 0 {
		threshold = 500
	}
	if minSpeech <= 0 {
		minSpeech = 3
	}
	if minSilence <= 0 {
		minSilence = 10
	}
	return &EnergyAnalyzer{Threshold: threshold, MinSpeech: minSpeech, MinSilence: minSilence}
}

func (e *EnergyAnalyzer) Analyze(pcm16 []byte, sampleRate int) (started, stopped bool) {
	if len(pcm16) < 2 {
		return false, false
	}
	var sum float64
	for i := 0; i+1 < len(pcm16); i += 2 {
		s := int16(binary.LittleEndian.Uint16(pcm16[i : i+2]))
		sum += float64(s) * float64(s)
	}
	n := float64(len(pcm16) / 2)
	rms := math.Sqrt(sum / n)

	if rms >= e.Threshold {
		e.speechRun++
		e.silenceRun = 0
		if !e.inSpeech && e.speechRun >= e.MinSpeech {
			e.inSpeech = true
			started = true
		}
	} else {
		e.silenceRun++
		e.speechRun = 0
		if e.inSpeech && e.silenceRun >= e.MinSilence {
			e.inSpeech = false
			stopped = true
		}
	}
	return started, stopped
}

// Processor wraps an Analyzer and emits VAD / user speaking frames.
type Processor struct {
	name     string
	analyzer Analyzer
}

func NewProcessor(name string, a Analyzer) *Processor {
	if a == nil {
		// Browser mics are often quiet; defaults tuned for 16 kHz s16le from WebSocket clients.
		a = NewEnergyAnalyzer(120, 2, 6)
	}
	return &Processor{name: name, analyzer: a}
}

func (p *Processor) Name() string { return p.name }

func (p *Processor) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch fr := f.(type) {
	case *frames.InputAudioRawFrame:
		st, sp := p.analyzer.Analyze(fr.Audio, fr.SampleRate)
		if st {
			emit.Down(&frames.VADUserStartedSpeakingFrame{})
			emit.Down(&frames.UserStartedSpeakingFrame{})
		}
		// Forward PCM before UserStoppedSpeaking so STT buffer includes this chunk before flush runs.
		emit.Down(f)
		if sp {
			emit.Down(&frames.VADUserStoppedSpeakingFrame{})
			emit.Down(&frames.UserStoppedSpeakingFrame{})
		}
	default:
		emit.Down(f)
	}
	return nil
}
