package vad

import (
	"encoding/binary"
	"math"
	"testing"
)

// scriptModel returns a fixed confidence for every window.
type scriptModel struct {
	sr, win int
	conf    float32
	calls   int
}

func (m *scriptModel) SampleRate() int    { return m.sr }
func (m *scriptModel) WindowSamples() int { return m.win }
func (m *scriptModel) Confidence([]float32) (float32, error) {
	m.calls++
	return m.conf, nil
}
func (m *scriptModel) Reset() { m.calls = 0 }

func pcmOf(n int, amp int16) []byte {
	b := make([]byte, n*2)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(amp))
	}
	return b
}

func TestStateFrameIndices(t *testing.T) {
	const sr, win = 16000, 512
	// frame = 512/16000 = 32 ms; round(0.2/0.032) = round(6.25) = 6.
	m := &scriptModel{sr: sr, win: win, conf: 1}
	a := NewStateAnalyzer(m, Params{Confidence: 0.7, StartSecs: 0.2, StopSecs: 0.2, MinVolume: 0})
	if a.StartFrames() != 6 || a.StopFrames() != 6 {
		t.Fatalf("frames start=%d stop=%d, want 6 and 6", a.StartFrames(), a.StopFrames())
	}
	one := pcmOf(win, 0)
	var startAt, stopAt int
	for i := 1; i <= 5; i++ {
		st, sp := a.Analyze(one, sr)
		if st || sp {
			t.Fatalf("window %d: started=%v stopped=%v, still starting", i, st, sp)
		}
		if a.State() != Starting {
			t.Fatalf("window %d state %s, want starting", i, a.State())
		}
	}
	st, sp := a.Analyze(one, sr)
	if !st || sp || a.State() != Speaking {
		t.Fatalf("window 6: started=%v stopped=%v state=%s", st, sp, a.State())
	}
	startAt = 6
	for i := 7; i <= 11; i++ {
		// confidence 0 → silence
		m.conf = 0
		st, sp = a.Analyze(one, sr)
		if st || sp {
			t.Fatalf("window %d: started=%v stopped=%v during stopping", i, st, sp)
		}
		if a.State() != Stopping {
			t.Fatalf("window %d state %s, want stopping", i, a.State())
		}
	}
	st, sp = a.Analyze(one, sr)
	if st || !sp || a.State() != Quiet {
		t.Fatalf("window 12: started=%v stopped=%v state=%s", st, sp, a.State())
	}
	stopAt = 12
	if startAt != 6 || stopAt != 12 {
		t.Fatalf("start %d stop %d", startAt, stopAt)
	}
}

func TestStateDipResetsStartCount(t *testing.T) {
	const sr, win = 16000, 512
	m := &scriptModel{sr: sr, win: win, conf: 1}
	a := NewStateAnalyzer(m, Params{Confidence: 0.7, StartSecs: 0.2, StopSecs: 0.2, MinVolume: 0})
	one := pcmOf(win, 0)
	for i := 0; i < 5; i++ {
		a.Analyze(one, sr)
	}
	m.conf = 0
	if _, sp := a.Analyze(one, sr); sp || a.State() != Quiet {
		t.Fatalf("dip: state %s", a.State())
	}
	m.conf = 1
	var started bool
	for i := 1; i <= 6; i++ {
		st, _ := a.Analyze(one, sr)
		if st {
			if i != 6 {
				t.Fatalf("restarted on window %d, want 6", i)
			}
			started = true
		}
	}
	if !started {
		t.Fatal("expected a fresh start after the dip")
	}
}

func TestStateResumeFromStopping(t *testing.T) {
	const sr, win = 16000, 512
	m := &scriptModel{sr: sr, win: win, conf: 1}
	a := NewStateAnalyzer(m, Params{Confidence: 0.7, StartSecs: 0.2, StopSecs: 0.2, MinVolume: 0})
	buf := pcmOf(win*6, 0)
	st, _ := a.Analyze(buf, sr)
	if !st {
		t.Fatal("expected start")
	}
	m.conf = 0
	a.Analyze(pcmOf(win*3, 0), sr)
	if a.State() != Stopping {
		t.Fatalf("state %s", a.State())
	}
	m.conf = 1
	st, sp := a.Analyze(pcmOf(win, 0), sr)
	if st || sp || a.State() != Speaking {
		t.Fatalf("resume started=%v stopped=%v state=%s", st, sp, a.State())
	}
}

func TestStateVolumeGate(t *testing.T) {
	const sr, win = 16000, 512
	m := &scriptModel{sr: sr, win: win, conf: 1}
	a := NewStateAnalyzer(m, Params{Confidence: 0.7, StartSecs: 0, StopSecs: 0.2, MinVolume: 0.6})
	// Silence never crosses the volume gate, whatever the confidence.
	for i := 0; i < 8; i++ {
		st, _ := a.Analyze(pcmOf(win, 0), sr)
		if st || a.State() != Quiet {
			t.Fatalf("silence started the machine on window %d (%s)", i, a.State())
		}
	}
	// Constant amplitude 20000 → about -4.3 dBFS → raw volume ~0.93.
	// Smoothing from 0 with factor 0.2 crosses 0.6 on the 5th window,
	// and StartSecs 0 confirms on that same window.
	loud := pcmOf(win, 20000)
	var startAt int
	for i := 1; i <= 8; i++ {
		st, _ := a.Analyze(loud, sr)
		if st {
			startAt = i
			break
		}
	}
	if startAt != 5 {
		t.Fatalf("loud start window %d, want 5", startAt)
	}
}

func TestStateConfidenceGate(t *testing.T) {
	m := &scriptModel{sr: 16000, win: 512, conf: 0.69}
	a := NewStateAnalyzer(m, Params{Confidence: 0.7, StartSecs: 0, StopSecs: 0, MinVolume: 0})
	if st, _ := a.Analyze(pcmOf(512, 0), 16000); st {
		t.Fatal("confidence below the gate started speech")
	}
	m.conf = 0.7
	if st, _ := a.Analyze(pcmOf(512, 0), 16000); !st {
		t.Fatal("confidence at the gate should start when start_secs is 0")
	}
}

func TestEnergyModelFeedsStateMachine(t *testing.T) {
	m := NewEnergyModel(16000, 160, 0.05)
	a := NewStateAnalyzer(m, Params{Confidence: 0.5, StartSecs: 0, StopSecs: 0, MinVolume: 0})
	if st, _ := a.Analyze(pcmOf(160, 0), 16000); st {
		t.Fatal("silence")
	}
	if st, _ := a.Analyze(pcmOf(160, 8000), 16000); !st {
		t.Fatal("expected energy model to start")
	}
}

func TestWindowVolumeScale(t *testing.T) {
	// Full-scale constant is 0 dBFS → 1. Amplitude 0 is 0.
	full := pcmOf(16, 32767)
	if v := WindowVolume(full); math.Abs(v-1) > 0.02 {
		t.Fatalf("full volume %v", v)
	}
	if v := WindowVolume(pcmOf(16, 0)); v != 0 {
		t.Fatalf("silence volume %v", v)
	}
}
