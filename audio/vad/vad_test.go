package vad

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func pcm16Sample(v int16, n int) []byte {
	b := make([]byte, 2*n)
	for i := 0; i < n; i++ {
		b[i*2] = byte(v)
		b[i*2+1] = byte(v >> 8)
	}
	return b
}

func TestEnergyAnalyzerSpeechStartStop(t *testing.T) {
	e := NewEnergyAnalyzer(100, 2, 2)
	st, sp := e.Analyze(pcm16Sample(0, 160), 16000)
	if st || sp {
		t.Fatalf("quiet: started=%v stopped=%v", st, sp)
	}
	// MinSpeech=2: first loud frame arms run, second loud triggers start
	st, sp = e.Analyze(pcm16Sample(3000, 160), 16000)
	if st || sp {
		t.Fatalf("loud1: started=%v stopped=%v", st, sp)
	}
	st, sp = e.Analyze(pcm16Sample(3000, 160), 16000)
	if !st || sp {
		t.Fatalf("loud2: started=%v stopped=%v (want start)", st, sp)
	}
	st, sp = e.Analyze(pcm16Sample(3000, 160), 16000)
	if st || sp {
		t.Fatalf("loud3: started=%v stopped=%v", st, sp)
	}
	st, sp = e.Analyze(pcm16Sample(0, 160), 16000)
	if st || sp {
		t.Fatalf("sil1: started=%v stopped=%v", st, sp)
	}
	st, sp = e.Analyze(pcm16Sample(0, 160), 16000)
	if st || !sp {
		t.Fatalf("sil2: started=%v stopped=%v (want stop)", st, sp)
	}
}

func TestProcessorForwardsNonAudio(t *testing.T) {
	p := NewProcessor("vad", NewEnergyAnalyzer(10000, 99, 99))
	var saw bool
	emit := processor.Emit{
		Down: func(f frames.Frame) {
			if _, ok := f.(*frames.TextFrame); ok {
				saw = true
			}
		},
	}
	tf := &frames.TextFrame{Text: "x"}
	if err := p.Process(context.Background(), tf, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Fatal("expected TextFrame forwarded")
	}
}
