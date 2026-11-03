//go:build !onnx

package silero

import (
	"errors"

	"github.com/rohitdas13595/llmpipe/audio/vad"
)

// Model is the Silero handle. This build has no ONNX runtime, so New fails
// and the energy VAD stays in use.
type Model struct{}

// New reports that Silero needs the onnx build tag.
func New(path string, sampleRate int) (*Model, error) {
	return nil, errors.New("silero: built without the onnx tag (set ONNXRUNTIME_LIB and rebuild with -tags onnx)")
}

func (m *Model) SampleRate() int { return 0 }

func (m *Model) WindowSamples() int { return 0 }

func (m *Model) Confidence([]float32) (float32, error) {
	return 0, errors.New("silero: built without the onnx tag")
}

func (m *Model) Reset() {}

func (m *Model) NoteWindow(float32, vad.State) {}

func (m *Model) Close() error { return nil }
