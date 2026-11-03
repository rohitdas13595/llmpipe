package smartturn

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/rohitdas13595/llmpipe/audio/turn"
)

func pcm(n int, amp int16) []byte {
	b := make([]byte, n*2)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(amp))
	}
	return b
}

func TestSilenceFallback(t *testing.T) {
	v := NewSilence(Params{StopSecs: 0.05, PreSpeechMS: 20, MaxDurationSecs: 8, Threshold: 0.5})
	const sr = 16000
	chunk := sr / 100 // 10ms
	for i := 0; i < 5; i++ {
		if v.AppendAudio(pcm(chunk, 1000), sr, true) != turn.Incomplete {
			t.Fatal("speech")
		}
	}
	var stopped bool
	for i := 0; i < 10; i++ {
		if v.AppendAudio(pcm(chunk, 0), sr, false) == turn.Complete {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("expected silence fallback")
	}
}

func TestRingKeepsTheEnd(t *testing.T) {
	v := NewSilence(Params{StopSecs: 3, PreSpeechMS: 0, MaxDurationSecs: 1, Threshold: 0.5})
	// Two seconds of audio must collapse to the last second (plus the
	// stop/pre trim ceiling, which is at least 8s of headroom when the
	// configured max is shorter — the trim floor is the feature window).
	if v.AppendAudio(pcm(sampleRate*2, 1), sampleRate, true) != turn.Incomplete {
		t.Fatal("append")
	}
	v.mu.Lock()
	n := len(v.buf)
	v.mu.Unlock()
	if n > sampleRate*12 {
		t.Fatalf("buffer grew without bound: %d samples", n)
	}
	seg := func() []float32 {
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.segmentLocked()
	}()
	if len(seg) > sampleRate {
		t.Fatalf("segment %d, want <= 1s", len(seg))
	}
}

func TestAnalyzeWithoutModel(t *testing.T) {
	v := NewSilence(DefaultParams())
	_, _, err := v.AnalyzeEndOfTurn(context.Background())
	if err == nil {
		t.Fatal("expected an error with no model")
	}
}

func TestNewV3MissingFile(t *testing.T) {
	_, err := NewV3("models/does-not-exist.onnx", DefaultParams())
	if err == nil {
		t.Fatal("expected an error")
	}
}
