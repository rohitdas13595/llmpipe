// Package onnx loads ONNX Runtime sessions when llmpipe is built with the
// "onnx" tag. The default build keeps the same API and returns an error, so
// the energy VAD and the silence turn fallback still compile and run.
//
// Set ONNXRUNTIME_LIB to the versioned shared library (for example
// libonnxruntime.so.1.22.0) before the first session is opened. The Go
// binding is github.com/yalue/onnxruntime_go, which tracks ORT 1.22.0.
package onnx

import (
	"os"
	"path/filepath"
)

// ModelPath looks for name under LLMPIPE_MODELS or a models/ directory walked
// up from the working directory. The second result is false when the file
// is not there.
func ModelPath(name string) (string, bool) {
	if dir := os.Getenv("LLMPIPE_MODELS"); dir != "" {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil { //nolint:gosec // G304: operator-supplied model directory
			return p, true
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for i := 0; i < 8; i++ {
		p := filepath.Join(dir, "models", name)
		if _, err := os.Stat(p); err == nil {
			return p, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}
