//go:build !onnx

package silero

import "testing"

func TestNewWithoutORT(t *testing.T) {
	_, err := New("silero_vad.onnx", 16000)
	if err == nil {
		t.Fatal("expected an error in the default build")
	}
}
