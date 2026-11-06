//go:build onnx

package silero

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
)

// TestWAVFixtures scores labelled speech clips when a directory of them is
// present. This tree does not ship a speech corpus, so the test skips and
// the 85% accuracy bar from the plan is not claimed.
func TestWAVFixtures(t *testing.T) {
	if _, ok := onnx.ModelPath("silero_vad.onnx"); !ok || os.Getenv("ONNXRUNTIME_LIB") == "" {
		t.Skip("silero model or ONNXRUNTIME_LIB unavailable")
	}
	matches, err := filepath.Glob("testdata/wavs/*.wav")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Skip("no labelled WAV fixtures in testdata/wavs; speech accuracy was not measured")
	}
	t.Fatalf("%d fixtures found but the scorer is not implemented; add labels before treating this as an accuracy run", len(matches))
}
