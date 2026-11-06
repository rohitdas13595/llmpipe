//go:build !onnx

package smartturn

import "errors"

// errNoModel is returned by AnalyzeEndOfTurn when no ONNX session is loaded.
var errNoModel = errors.New("smart turn: no ONNX model (build with -tags onnx and set ONNXRUNTIME_LIB)")

// NewV3 reports that this build has no ONNX runtime. Use NewSilence for the
// silence fallback, which does not need a model.
func NewV3(modelPath string, p Params) (*V3, error) {
	return nil, errNoModel
}
