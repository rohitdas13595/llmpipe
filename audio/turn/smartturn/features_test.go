package smartturn

import (
	"math"
	"testing"
)

func TestLogMelSilence(t *testing.T) {
	out := LogMel(nil)
	if len(out) != nMel*FeatureFrames {
		t.Fatalf("len %d", len(out))
	}
	for i, v := range out {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			t.Fatalf("non-finite at %d", i)
		}
		if math.Abs(float64(v)+1.5) > 1e-5 {
			t.Fatalf("silence[%d]=%v, want -1.5", i, v)
		}
	}
}

func TestLogMelSineMatchesNumpy(t *testing.T) {
	// 8 seconds of 440 Hz at amplitude 0.25, with sin evaluated in float64
	// and then cast to float32. Checked against the numpy path Pipecat vendors
	// in _whisper_features.py: on that identical buffer the full 80×800 matrix
	// agreed within 1e-5. The samples below are that reference.
	const n = 16000 * 8
	samples := make([]float32, n)
	for i := range samples {
		samples[i] = 0.25 * float32(math.Sin(2*math.Pi*440*float64(i)/16000))
	}
	out := LogMel(samples)
	if len(out) != nMel*FeatureFrames {
		t.Fatalf("len %d", len(out))
	}
	checks := []struct {
		i    int
		want float64
	}{
		{0, 1.2090509},
		{100, -0.3360241},
		{800, 1.2123928},
		{len(out) - 1, -0.3360241},
	}
	for _, c := range checks {
		if math.Abs(float64(out[c.i])-c.want) > 1e-4 {
			t.Fatalf("out[%d]=%v, want %v (±1e-4)", c.i, out[c.i], c.want)
		}
	}
	min, max := float64(out[0]), float64(out[0])
	for _, v := range out {
		if float64(v) < min {
			min = float64(v)
		}
		if float64(v) > max {
			max = float64(v)
		}
	}
	if math.Abs(min+0.3360241) > 1e-4 || math.Abs(max-1.663976) > 1e-4 {
		t.Fatalf("range %v..%v", min, max)
	}
}

func TestLogMelShapeAndFinite(t *testing.T) {
	samples := make([]float32, 16000)
	for i := range samples {
		samples[i] = 0.2 * float32(math.Sin(2*math.Pi*440*float64(i)/sampleRate))
	}
	out := LogMel(samples)
	if len(out) != 80*800 {
		t.Fatalf("len %d", len(out))
	}
	var sum float64
	for _, v := range out {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			t.Fatal("non-finite")
		}
		sum += math.Abs(float64(v))
	}
	if sum == 0 {
		t.Fatal("sine produced an all-zero mel")
	}
}

func TestReflectPad(t *testing.T) {
	// np.pad([1,2,3,4,5], (2, 2), mode="reflect") == [3,2,1,2,3,4,5,4,3]
	got := reflectPad([]float32{1, 2, 3, 4, 5}, 2)
	want := []float64{3, 2, 1, 2, 3, 4, 5, 4, 3}
	if len(got) != len(want) {
		t.Fatalf("len %d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pad[%d]=%v", i, got[i])
		}
	}
}

func TestHannPeriodic(t *testing.T) {
	// np.hanning(5)[:-1] for a length-4 window.
	w := make([]float64, 4)
	for i := 0; i < 4; i++ {
		w[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/4)
	}
	want := []float64{0, 0.5, 1, 0.5}
	for i := range want {
		if math.Abs(w[i]-want[i]) > 1e-12 {
			t.Fatalf("hann[%d]=%v", i, w[i])
		}
	}
}

func FuzzLogMel(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 1, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0x7f}) // non-finite float32 bits, coerced
	f.Fuzz(func(t *testing.T, b []byte) {
		n := len(b) / 4
		if n > 4000 {
			n = 4000
		}
		samples := make([]float32, n)
		for i := 0; i < n; i++ {
			samples[i] = math.Float32frombits(uint32(b[i*4]) | uint32(b[i*4+1])<<8 | uint32(b[i*4+2])<<16 | uint32(b[i*4+3])<<24)
		}
		out := LogMel(samples)
		if len(out) != nMel*FeatureFrames {
			t.Fatalf("len %d", len(out))
		}
		for _, v := range out {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				t.Fatal("non-finite feature")
			}
		}
	})
}
