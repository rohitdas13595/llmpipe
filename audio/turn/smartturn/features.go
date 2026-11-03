// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/audio/turn/smart_turn/_whisper_features.py,
// which vendors the numpy path of Hugging Face Transformers
// (src/transformers/models/whisper/feature_extraction_whisper.py and
// src/transformers/audio_utils.py).
//
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Copyright 2022 The HuggingFace Inc. team. Apache License 2.0.
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

// Package smartturn extracts Whisper log-mel features and runs Smart Turn v3.
package smartturn

import (
	"math"

	"gonum.org/v1/gonum/dsp/fourier"
)

const (
	sampleRate = 16000
	nFFT       = 400
	hop        = 160
	nMel       = 80
	// FeatureSamples is 8 seconds at 16 kHz, the Smart Turn window.
	FeatureSamples = sampleRate * 8
	// FeatureFrames is the time axis of LogMel ([1, 80, 800]).
	FeatureFrames = 800
	melFloor      = 1e-10
	normVarEps    = 1e-7
)

// LogMel returns Whisper-style log-mel features for 16 kHz mono audio.
// The result has length nMel*FeatureFrames (80*800), row-major, matching an
// ONNX input of shape [1, 80, 800]. Audio shorter than 8 seconds is zero-padded
// at the end; longer audio keeps the leading 8 seconds (Whisper's rule).
// Smart Turn itself keeps the last 8 seconds before calling LogMel.
// Samples are zero-mean unit-variance normalised first. Non-finite samples
// are treated as silence so the matrix stays finite.
func LogMel(samples []float32) []float32 {
	return logMel(samples, true)
}

func logMel(samples []float32, normalize bool) []float32 {
	x := make([]float32, FeatureSamples)
	n := len(samples)
	if n > FeatureSamples {
		n = FeatureSamples
	}
	for i := 0; i < n; i++ {
		v := samples[i]
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			continue
		}
		x[i] = v
	}
	if normalize {
		whiten(x)
	}
	mags := powerSpec(x)
	filters := melFilters()
	// One extra frame is produced by the centred STFT and then dropped,
	// matching transformers' log_spec[:, :-1].
	rawFrames := FeatureFrames + 1
	spec := make([]float64, nMel*rawFrames)
	bins := nFFT/2 + 1
	for t := 0; t < rawFrames; t++ {
		col := mags[t*bins : (t+1)*bins]
		for m := 0; m < nMel; m++ {
			var acc float64
			row := filters[m*bins : (m+1)*bins]
			for k := 0; k < bins; k++ {
				acc += row[k] * col[k]
			}
			if acc < melFloor {
				acc = melFloor
			}
			spec[m*rawFrames+t] = math.Log10(acc)
		}
	}
	kept := nMel * FeatureFrames
	logSpec := make([]float64, kept)
	var maxV float64
	first := true
	for m := 0; m < nMel; m++ {
		for t := 0; t < FeatureFrames; t++ {
			v := spec[m*rawFrames+t]
			logSpec[m*FeatureFrames+t] = v
			if first || v > maxV {
				maxV = v
				first = false
			}
		}
	}
	floor := maxV - 8
	out := make([]float32, kept)
	for i, v := range logSpec {
		if v < floor {
			v = floor
		}
		out[i] = float32((v + 4) / 4)
	}
	return out
}

func whiten(x []float32) {
	n := float32(len(x))
	if n == 0 {
		return
	}
	var sum float32
	for _, v := range x {
		sum += v
	}
	mean := sum / n
	var acc float32
	for i, v := range x {
		d := v - mean
		x[i] = d
		acc += d * d
	}
	inv := float32(1 / math.Sqrt(float64(acc/n)+normVarEps))
	for i := range x {
		x[i] *= inv
	}
}

// powerSpec returns a centred power spectrogram, frame-major
// (rawFrames * (nFFT/2+1)).
func powerSpec(wave []float32) []float64 {
	pad := nFFT / 2
	padded := reflectPad(wave, pad)
	win := hannWindow()
	fft := fourier.NewFFT(nFFT)
	frame := make([]float64, nFFT)
	coeff := make([]complex128, nFFT/2+1)
	bins := nFFT/2 + 1
	rawFrames := 1 + (len(padded)-nFFT)/hop
	mags := make([]float64, rawFrames*bins)
	for t := 0; t < rawFrames; t++ {
		base := t * hop
		for i := 0; i < nFFT; i++ {
			frame[i] = padded[base+i] * win[i]
		}
		coeff = fft.Coefficients(coeff, frame)
		row := mags[t*bins : (t+1)*bins]
		for k, c := range coeff {
			re := real(c)
			im := imag(c)
			row[k] = re*re + im*im
		}
	}
	return mags
}

func reflectPad(x []float32, pad int) []float64 {
	n := len(x)
	out := make([]float64, n+2*pad)
	for i, v := range x {
		out[pad+i] = float64(v)
	}
	for i := 0; i < pad; i++ {
		out[pad-1-i] = out[pad+1+i]
		out[pad+n+i] = out[pad+n-2-i]
	}
	return out
}

func hannWindow() []float64 {
	// np.hanning(nFFT+1)[:-1], the periodic Hann window.
	w := make([]float64, nFFT)
	for i := 0; i < nFFT; i++ {
		w[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(nFFT))
	}
	return w
}

// melFilters is (nMel, nFFT/2+1), Slaney-normalised.
var cachedMel []float64

func melFilters() []float64 {
	if cachedMel != nil {
		return cachedMel
	}
	bins := nFFT/2 + 1
	melMin := hzToMel(0)
	melMax := hzToMel(float64(sampleRate / 2))
	melFreqs := linspace(melMin, melMax, nMel+2)
	filterHz := make([]float64, len(melFreqs))
	for i, m := range melFreqs {
		filterHz[i] = melToHz(m)
	}
	fftHz := linspace(0, float64(sampleRate/2), bins)
	diff := make([]float64, len(filterHz)-1)
	for i := range diff {
		diff[i] = filterHz[i+1] - filterHz[i]
	}
	// Store transposed: row m, bin k.
	filters := make([]float64, nMel*bins)
	for k := 0; k < bins; k++ {
		for m := 0; m < nMel; m++ {
			down := -(filterHz[m] - fftHz[k]) / diff[m]
			up := (filterHz[m+2] - fftHz[k]) / diff[m+1]
			v := down
			if up < v {
				v = up
			}
			if v < 0 {
				v = 0
			}
			enorm := 2 / (filterHz[m+2] - filterHz[m])
			filters[m*bins+k] = v * enorm
		}
	}
	cachedMel = filters
	return filters
}

func hzToMel(freq float64) float64 {
	const minLogHz = 1000.0
	const minLogMel = 15.0
	logstep := 27.0 / math.Log(6.4)
	mel := 3 * freq / 200
	if freq >= minLogHz {
		mel = minLogMel + math.Log(freq/minLogHz)*logstep
	}
	return mel
}

func melToHz(mel float64) float64 {
	const minLogHz = 1000.0
	const minLogMel = 15.0
	logstep := math.Log(6.4) / 27.0
	if mel >= minLogMel {
		return minLogHz * math.Exp(logstep*(mel-minLogMel))
	}
	return 200 * mel / 3
}

func linspace(a, b float64, n int) []float64 {
	out := make([]float64, n)
	if n == 1 {
		out[0] = a
		return out
	}
	step := (b - a) / float64(n-1)
	for i := 0; i < n; i++ {
		out[i] = a + step*float64(i)
	}
	out[n-1] = b
	return out
}
