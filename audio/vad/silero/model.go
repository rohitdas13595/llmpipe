// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/audio/vad/silero.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// The weights are Silero VAD v5 (MIT). See THIRD_PARTY_NOTICES.md.
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

//go:build onnx

package silero

import (
	"fmt"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
	"github.com/rohitdas13595/llmpipe/audio/vad"
	ort "github.com/yalue/onnxruntime_go"
)

const (
	resetIntervalSec = 2.0
	resetMaxConf     = float32(0.1)
	resetLowWindows  = 5
)

// Model is Silero VAD v5. Sample rates are 8000 or 16000. Each Confidence
// call consumes one window (256 or 512 samples) and carries the recurrent
// state plus the trailing context the model prepends.
type Model struct {
	sess              *onnx.Session
	rate              int
	window            int
	contextN          int
	context           []float32
	inT               *ort.Tensor[float32]
	stateT            *ort.Tensor[float32]
	srT               *ort.Tensor[int64]
	outT              *ort.Tensor[float32]
	stateOut          *ort.Tensor[float32]
	windowsSinceReset int
	lowConf           int
}

// New loads the model. sampleRate must be 8000 or 16000.
func New(path string, sampleRate int) (*Model, error) {
	if sampleRate != 8000 && sampleRate != 16000 {
		return nil, fmt.Errorf("silero: sample rate must be 8000 or 16000, got %d", sampleRate)
	}
	sess, err := onnx.NewSession(path, 1,
		[]string{"input", "state", "sr"},
		[]string{"output", "stateN"})
	if err != nil {
		return nil, err
	}
	m := &Model{sess: sess, rate: sampleRate}
	m.window = 512
	m.contextN = 64
	if sampleRate == 8000 {
		m.window = 256
		m.contextN = 32
	}
	m.context = make([]float32, m.contextN)
	if err := m.alloc(); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

func (m *Model) alloc() error {
	var err error
	m.inT, err = ort.NewEmptyTensor[float32](ort.NewShape(1, int64(m.contextN+m.window)))
	if err != nil {
		return err
	}
	m.stateT, err = ort.NewEmptyTensor[float32](ort.NewShape(2, 1, 128))
	if err != nil {
		return err
	}
	m.srT, err = ort.NewTensor(ort.NewShape(1), []int64{int64(m.rate)})
	if err != nil {
		return err
	}
	m.outT, err = ort.NewEmptyTensor[float32](ort.NewShape(1, 1))
	if err != nil {
		return err
	}
	m.stateOut, err = ort.NewEmptyTensor[float32](ort.NewShape(2, 1, 128))
	if err != nil {
		return err
	}
	return nil
}

func (m *Model) SampleRate() int { return m.rate }

func (m *Model) WindowSamples() int { return m.window }

// Confidence scores one window. The float32 samples are in [-1, 1].
func (m *Model) Confidence(samples []float32) (float32, error) {
	if len(samples) != m.window {
		return 0, fmt.Errorf("silero: want %d samples, got %d", m.window, len(samples))
	}
	in := m.inT.GetData()
	copy(in, m.context)
	copy(in[m.contextN:], samples)
	err := m.sess.Run(
		[]ort.Value{m.inT, m.stateT, m.srT},
		[]ort.Value{m.outT, m.stateOut},
	)
	if err != nil {
		return 0, err
	}
	copy(m.stateT.GetData(), m.stateOut.GetData())
	copy(m.context, in[len(in)-m.contextN:])
	data := m.outT.GetData()
	if len(data) == 0 {
		return 0, fmt.Errorf("silero: empty output")
	}
	return data[0], nil
}

// Reset clears the recurrent state and the audio context.
func (m *Model) Reset() {
	if m.stateT != nil {
		for i := range m.stateT.GetData() {
			m.stateT.GetData()[i] = 0
		}
	}
	for i := range m.context {
		m.context[i] = 0
	}
}

// NoteWindow resets the recurrent state about every two seconds, and only
// while the VAD state before this window was Quiet and recent windows were
// clearly non-speech. state is that previous state.
func (m *Model) NoteWindow(confidence float32, state vad.State) {
	m.windowsSinceReset++
	if confidence < resetMaxConf {
		m.lowConf++
	} else {
		m.lowConf = 0
	}
	if m.windowsSinceReset*m.window >= int(resetIntervalSec*float64(m.rate)) &&
		state == vad.Quiet && m.lowConf >= resetLowWindows {
		m.Reset()
		m.windowsSinceReset = 0
	}
}

// Close releases tensors and the session.
func (m *Model) Close() error {
	if m == nil {
		return nil
	}
	var err error
	destroy := func(d interface{ Destroy() error }) {
		if d == nil {
			return
		}
		if e := d.Destroy(); e != nil && err == nil {
			err = e
		}
	}
	if m.inT != nil {
		destroy(m.inT)
	}
	if m.stateT != nil {
		destroy(m.stateT)
	}
	if m.srT != nil {
		destroy(m.srT)
	}
	if m.outT != nil {
		destroy(m.outT)
	}
	if m.stateOut != nil {
		destroy(m.stateOut)
	}
	m.inT, m.stateT, m.srT, m.outT, m.stateOut = nil, nil, nil, nil, nil
	if m.sess != nil {
		if e := m.sess.Close(); e != nil && err == nil {
			err = e
		}
		m.sess = nil
	}
	return err
}
