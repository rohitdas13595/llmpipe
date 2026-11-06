//go:build !onnx

package onnx

import "fmt"

// ORTVersion is the runtime a tagged build expects. The default build does
// not link it.
const ORTVersion = "1.22.0"

// Linked reports that this binary does not include ONNX Runtime.
func Linked() bool { return false }

// Init reports that this binary was built without ONNX Runtime.
func Init() error {
	return fmt.Errorf("onnx: built without the onnx tag (set ONNXRUNTIME_LIB to libonnxruntime.so.%s and rebuild with -tags onnx)", ORTVersion)
}

// Info is one model input or output. Present so both builds share the type.
type Info struct {
	Name string
}

// Session is the loaded-model handle. In this build every constructor fails.
type Session struct{}

// NewSession reports that ONNX Runtime is not linked.
func NewSession(path string, threads int, inputs, outputs []string) (*Session, error) {
	return nil, Init()
}

// Close is a no-op.
func (s *Session) Close() error { return nil }
