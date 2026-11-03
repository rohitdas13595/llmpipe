//go:build onnx

package smartturn

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
	"github.com/rohitdas13595/llmpipe/audio/turn"
)

func TestSmartTurnSilenceProbability(t *testing.T) {
	path, ok := onnx.ModelPath("smart-turn-v3.2-cpu.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("smart turn model or ONNXRUNTIME_LIB unavailable")
	}
	v, err := NewV3(path, DefaultParams())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	// Eight seconds of silence. The probability just has to be a finite
	// classifier output; the labelled-WAV accuracy bar needs real speech.
	pcm := make([]byte, sampleRate*8*2)
	if v.AppendAudio(pcm, sampleRate, true) != turn.Incomplete {
		t.Fatal("append")
	}
	res, prob, err := v.AnalyzeEndOfTurn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if prob < 0 || prob > 1 {
		t.Fatalf("probability %v", prob)
	}
	if res != turn.Complete && res != turn.Incomplete {
		t.Fatalf("result %v", res)
	}
	t.Logf("silence probability %v result %v", prob, res)
}

func TestSmartTurnInferUnder500ms(t *testing.T) {
	path, ok := onnx.ModelPath("smart-turn-v3.2-cpu.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("smart turn model or ONNXRUNTIME_LIB unavailable")
	}
	v, err := NewV3(path, DefaultParams())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	pcm := make([]byte, sampleRate*2)
	_ = v.AppendAudio(pcm, sampleRate, true)
	if _, _, err := v.AnalyzeEndOfTurn(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, _, err := v.AnalyzeEndOfTurn(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("inference %s, want < 500ms", elapsed)
	}
}

func BenchmarkSmartTurnInfer(b *testing.B) {
	path, ok := onnx.ModelPath("smart-turn-v3.2-cpu.onnx")
	if !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		b.Skip("smart turn model or ONNXRUNTIME_LIB unavailable")
	}
	v, err := NewV3(path, DefaultParams())
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	pcm := make([]byte, sampleRate)
	_ = v.AppendAudio(pcm, sampleRate, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := v.AnalyzeEndOfTurn(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
