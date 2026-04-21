package vad

import (
	"encoding/binary"
	"testing"
)

// pcmTone writes a loud tone (high RMS) into buf s16le mono.
func pcmTone(buf []byte, amp int16) {
	for i := 0; i+1 < len(buf); i += 2 {
		binary.LittleEndian.PutUint16(buf[i:], uint16(amp))
	}
}

func pcmSilent(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func TestEnergyAnalyzer_SilenceStopMS(t *testing.T) {
	const sr = 16000
	chunk := sr / 100 * 2 // 10ms chunk = 320 bytes

	e := NewEnergyAnalyzer(2000, 1, 9999)
	e.SilenceStopMS = 50

	loud := make([]byte, chunk)
	pcmTone(loud, 5000)
	quiet := make([]byte, chunk)
	pcmSilent(quiet)

	var started, stopped bool
	// enter speech
	for i := 0; i < 5; i++ {
		st, sp := e.Analyze(loud, sr)
		if st {
			started = true
		}
		if sp {
			stopped = true
		}
	}
	if !started {
		t.Fatal("expected speech start")
	}
	// ~60ms silence should trigger stop (50ms threshold)
	for i := 0; i < 10; i++ {
		_, sp := e.Analyze(quiet, sr)
		if sp {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("expected end-of-turn after silence ms")
	}
}

func TestEnergyAnalyzer_FrameBasedSilence(t *testing.T) {
	const sr = 16000
	chunk := sr / 100 * 2
	e := NewEnergyAnalyzer(2000, 1, 3)
	if e.SilenceStopMS != 0 {
		t.Fatal("default SilenceStopMS should be 0")
	}
	loud := make([]byte, chunk)
	pcmTone(loud, 5000)
	quiet := make([]byte, chunk)
	pcmSilent(quiet)
	for range 5 {
		e.Analyze(loud, sr)
	}
	var stopped bool
	for range 10 {
		_, sp := e.Analyze(quiet, sr)
		if sp {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("expected frame-based stop")
	}
}
