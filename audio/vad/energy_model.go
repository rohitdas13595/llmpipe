package vad

import "math"

// EnergyModel is a Model that scores a window by its RMS. It lets the state
// machine run with no ONNX runtime. Confidence is 1 when the RMS of the
// float32 samples is at least Threshold, otherwise 0.
type EnergyModel struct {
	Rate      int
	Window    int
	Threshold float32
}

// NewEnergyModel returns a model. Non-positive rate, window, or threshold
// become 16000, 512, and 0.05.
func NewEnergyModel(rate, window int, threshold float32) *EnergyModel {
	if rate <= 0 {
		rate = 16000
	}
	if window <= 0 {
		window = 512
	}
	if threshold <= 0 {
		threshold = 0.05
	}
	return &EnergyModel{Rate: rate, Window: window, Threshold: threshold}
}

func (m *EnergyModel) SampleRate() int { return m.Rate }

func (m *EnergyModel) WindowSamples() int { return m.Window }

func (m *EnergyModel) Confidence(samples []float32) (float32, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	var sum float64
	for _, v := range samples {
		sum += float64(v) * float64(v)
	}
	rms := math.Sqrt(sum / float64(len(samples)))
	if float32(rms) >= m.Threshold {
		return 1, nil
	}
	return 0, nil
}

func (m *EnergyModel) Reset() {}
