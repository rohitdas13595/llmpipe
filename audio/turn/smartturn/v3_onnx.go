//go:build onnx

package smartturn

import (
	"errors"
	"fmt"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
	ort "github.com/yalue/onnxruntime_go"
)

var errNoModel = errors.New("smart turn: model is not loaded")

// NewV3 loads smart-turn-v3.2-cpu.onnx. Inference runs when AnalyzeEndOfTurn
// is called; the turn processor is expected to do that on a worker goroutine
// and feed the result back with QueueSelf.
func NewV3(modelPath string, p Params) (*V3, error) {
	v := NewSilence(p)
	sess, err := onnx.NewSession(modelPath, 1, []string{"input_features"}, []string{"logits"})
	if err != nil {
		return nil, err
	}
	v.closer = sess.Close
	v.predict = func(mel []float32) (float32, error) {
		if len(mel) != nMel*FeatureFrames {
			return 0, fmt.Errorf("smart turn: mel len %d, want %d", len(mel), nMel*FeatureFrames)
		}
		in, err := ort.NewTensor(ort.NewShape(1, nMel, FeatureFrames), mel)
		if err != nil {
			return 0, err
		}
		defer func() { _ = in.Destroy() }()
		outs := []ort.Value{nil}
		if err := sess.Run([]ort.Value{in}, outs); err != nil {
			return 0, err
		}
		if outs[0] != nil {
			defer func() { _ = outs[0].Destroy() }()
		}
		ft, ok := outs[0].(*ort.Tensor[float32])
		if !ok || len(ft.GetData()) == 0 {
			return 0, fmt.Errorf("smart turn: unexpected output %T", outs[0])
		}
		return ft.GetData()[0], nil
	}
	return v, nil
}
