// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/audio/turn/smart_turn/base_smart_turn.py and local_smart_turn_v3.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

package smartturn

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/internal/resample"
)

// Params configures the silence fallback and the audio window sent to the model.
// A zero field selects the Pipecat default (stop 3s, 500 ms pre-speech, 8s max,
// threshold 0.5).
type Params struct {
	StopSecs        float64
	PreSpeechMS     float64
	MaxDurationSecs float64
	Threshold       float32
}

// DefaultParams matches Pipecat SmartTurnParams.
func DefaultParams() Params {
	return Params{StopSecs: 3, PreSpeechMS: 500, MaxDurationSecs: 8, Threshold: 0.5}
}

func (p Params) withDefaults() Params {
	d := DefaultParams()
	if p.StopSecs == 0 {
		p.StopSecs = d.StopSecs
	}
	if p.PreSpeechMS == 0 {
		p.PreSpeechMS = d.PreSpeechMS
	}
	if p.MaxDurationSecs == 0 {
		p.MaxDurationSecs = d.MaxDurationSecs
	}
	if p.Threshold == 0 {
		p.Threshold = d.Threshold
	}
	return p
}

// V3 is a Smart Turn v3 analyzer. AppendAudio is safe to call from the
// processor goroutine while AnalyzeEndOfTurn runs on a worker.
// Without a model (NewSilence, or NewV3 in a build without the onnx tag)
// AppendAudio still ends the turn after StopSecs of silence.
//
// Timing is counted in samples at 16 kHz, not on the wall clock, so a slow
// host cannot drop the pre-speech buffer. Non-16 kHz input is linearly
// resampled.
type V3 struct {
	mu        sync.Mutex
	p         Params
	buf       []int16
	speechAt  int
	silence   int
	triggered bool
	predict   func(mel []float32) (float32, error)
	closer    func() error
}

// Close releases the ONNX session. It is safe to call more than once.
func (v *V3) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.predict = nil
	if v.closer == nil {
		return nil
	}
	err := v.closer()
	v.closer = nil
	return err
}

// NewSilence returns an analyzer with only the silence fallback.
func NewSilence(p Params) *V3 {
	return &V3{p: p.withDefaults(), speechAt: -1}
}

// AppendAudio buffers one mono s16le chunk. isSpeech should follow the VAD.
func (v *V3) AppendAudio(pcm []byte, sr int, isSpeech bool) turn.Result {
	v.mu.Lock()
	defer v.mu.Unlock()
	if sr <= 0 {
		sr = sampleRate
	}
	if sr != sampleRate {
		pcm = resample.LinearS16LE(pcm, sr, sampleRate)
	}
	n := len(pcm) / 2
	if n == 0 {
		return turn.Incomplete
	}
	if cap(v.buf)-len(v.buf) < n {
		next := make([]int16, len(v.buf), len(v.buf)+n+sampleRate)
		copy(next, v.buf)
		v.buf = next
	}
	for i := 0; i < n; i++ {
		v.buf = append(v.buf, int16(binary.LittleEndian.Uint16(pcm[i*2:])))
	}
	if isSpeech {
		if !v.triggered {
			v.speechAt = len(v.buf) - n
			v.triggered = true
		}
		v.silence = 0
	} else if v.triggered {
		v.silence += n
		stopSamples := int(v.p.StopSecs * sampleRate)
		if v.silence >= stopSamples {
			v.clearLocked()
			return turn.Complete
		}
	}
	v.trimLocked()
	return turn.Incomplete
}

// AnalyzeEndOfTurn runs the ONNX model on the current speech segment.
// A probability above Threshold is Complete. The silence fallback is handled
// by AppendAudio and is not repeated here.
func (v *V3) AnalyzeEndOfTurn(ctx context.Context) (turn.Result, float32, error) {
	if err := ctx.Err(); err != nil {
		return turn.Incomplete, 0, err
	}
	v.mu.Lock()
	seg := v.segmentLocked()
	predict := v.predict
	threshold := v.p.Threshold
	v.mu.Unlock()
	if predict == nil {
		return turn.Incomplete, 0, errNoModel
	}
	if len(seg) > FeatureSamples {
		seg = seg[len(seg)-FeatureSamples:]
	}
	mel := LogMel(seg)
	prob, err := predict(mel)
	if err != nil {
		return turn.Incomplete, 0, err
	}
	if prob > threshold {
		v.mu.Lock()
		v.clearLocked()
		v.mu.Unlock()
		return turn.Complete, prob, nil
	}
	return turn.Incomplete, prob, nil
}

// Clear drops the buffer and the speech flag.
func (v *V3) Clear() {
	v.mu.Lock()
	v.clearLocked()
	v.mu.Unlock()
}

func (v *V3) clearLocked() {
	v.buf = v.buf[:0]
	v.speechAt = -1
	v.silence = 0
	v.triggered = false
}

func (v *V3) trimLocked() {
	maxSec := v.p.PreSpeechMS/1000 + v.p.StopSecs + v.p.MaxDurationSecs
	maxN := int(maxSec * sampleRate)
	if maxN < FeatureSamples {
		maxN = FeatureSamples
	}
	if len(v.buf) <= maxN {
		return
	}
	drop := len(v.buf) - maxN
	copy(v.buf, v.buf[drop:])
	v.buf = v.buf[:maxN]
	if v.speechAt >= 0 {
		v.speechAt -= drop
		if v.speechAt < 0 {
			v.speechAt = 0
		}
	}
}

func (v *V3) segmentLocked() []float32 {
	if len(v.buf) == 0 {
		return nil
	}
	start := 0
	if v.triggered && v.speechAt >= 0 {
		pre := int(v.p.PreSpeechMS / 1000 * sampleRate)
		start = v.speechAt - pre
		if start < 0 {
			start = 0
		}
	}
	out := make([]float32, len(v.buf)-start)
	for i, s := range v.buf[start:] {
		out[i] = float32(s) / 32768
	}
	maxN := int(v.p.MaxDurationSecs * sampleRate)
	if maxN > 0 && len(out) > maxN {
		out = out[len(out)-maxN:]
	}
	return out
}

// Compile-time check that V3 is a turn analyzer.
var _ turn.Analyzer = (*V3)(nil)
