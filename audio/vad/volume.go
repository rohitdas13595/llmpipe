package vad

import (
	"encoding/binary"
	"math"
)

// VolumeSmooth is the exponential-smoothing factor applied to each analysis
// window (Pipecat VADAnalyzer._smoothing_factor).
const VolumeSmooth = 0.2

// WindowVolume maps mono s16le PCM to a level in [0, 1].
// 0 dBFS is 1 and -60 dBFS is 0.
//
// Pipecat measures ITU-R BS.1770 loudness over a 400 ms window. That meter
// depends on a Python package this tree does not vendor, so the state machine
// gates on the smoothed dBFS of each analysis window instead. The scale still
// matches VADParams.MinVolume (default 0.6).
func WindowVolume(pcm []byte) float64 {
	n := len(pcm) / 2
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(pcm[i*2:]))
		x := float64(s) / 32768
		sum += x * x
	}
	rms := math.Sqrt(sum / float64(n))
	if rms < 1e-10 {
		return 0
	}
	db := 20 * math.Log10(rms)
	v := (db + 60) / 60
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// SmoothVolume applies exponential smoothing: prev + factor*(sample-prev).
func SmoothVolume(prev, sample, factor float64) float64 {
	return prev + factor*(sample-prev)
}
