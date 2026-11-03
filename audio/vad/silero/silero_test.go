//go:build onnx

package silero

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
	"github.com/rohitdas13595/llmpipe/audio/vad"
)

type goldenFile struct {
	Cases []struct {
		SR      int         `json:"sr"`
		Probs   []float64   `json:"probs"`
		Windows [][]float64 `json:"windows"`
	} `json:"cases"`
}

func TestGoldenAgainstPython(t *testing.T) {
	path, ok := onnx.ModelPath("silero_vad.onnx")
	if !ok {
		t.Skip("silero_vad.onnx not downloaded (make models)")
	}
	if os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("ONNXRUNTIME_LIB not set")
	}
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g goldenFile
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	for _, c := range g.Cases {
		m, err := New(path, c.SR)
		if err != nil {
			t.Fatal(err)
		}
		for i, w := range c.Windows {
			samples := make([]float32, len(w))
			for j, v := range w {
				samples[j] = float32(v)
			}
			got, err := m.Confidence(samples)
			if err != nil {
				t.Fatal(err)
			}
			want := c.Probs[i]
			if math.Abs(float64(got)-want) > 0.01 {
				t.Fatalf("sr %d window %d: got %v want %v (±0.01)", c.SR, i, got, want)
			}
		}
		_ = m.Close()
	}
}

func TestResetRestoresQuietWindow(t *testing.T) {
	path, ok := onnx.ModelPath("silero_vad.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("silero model or ONNXRUNTIME_LIB unavailable")
	}
	m, err := New(path, 16000)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	silence := make([]float32, m.WindowSamples())
	first, err := m.Confidence(silence)
	if err != nil {
		t.Fatal(err)
	}
	m.Reset()
	// 63 windows is just over 2 seconds at 512 samples / 16 kHz.
	var last float32
	for i := 0; i < 63; i++ {
		last, err = m.Confidence(silence)
		if err != nil {
			t.Fatal(err)
		}
		m.NoteWindow(last, vad.Quiet)
	}
	again, err := m.Confidence(silence)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(again-first)) > 0.01 {
		t.Fatalf("after the quiet reset got %v, fresh window was %v", again, first)
	}
}

func TestWindowUnder5ms(t *testing.T) {
	path, ok := onnx.ModelPath("silero_vad.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("silero model or ONNXRUNTIME_LIB unavailable")
	}
	m, err := New(path, 16000)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	samples := make([]float32, m.WindowSamples())
	if _, err := m.Confidence(samples); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	const n = 20
	for i := 0; i < n; i++ {
		if _, err := m.Confidence(samples); err != nil {
			t.Fatal(err)
		}
	}
	if each := time.Since(start) / n; each > 5*time.Millisecond {
		t.Fatalf("mean window %s, want < 5ms", each)
	}
}

func BenchmarkSileroWindow(b *testing.B) {
	path, ok := onnx.ModelPath("silero_vad.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		b.Skip("silero model or ONNXRUNTIME_LIB unavailable")
	}
	m, err := New(path, 16000)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	samples := make([]float32, m.WindowSamples())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.Confidence(samples); err != nil {
			b.Fatal(err)
		}
	}
}
