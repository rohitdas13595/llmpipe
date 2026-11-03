// Package silero is the Silero VAD v5 model.
//
// New returns an error unless llmpipe is built with -tags onnx and
// ONNXRUNTIME_LIB points at libonnxruntime.so. The energy VAD remains the
// fallback in the default build.
package silero
