package turn

import (
	"encoding/binary"
	"testing"
)

func TestSilenceAnalyzer(t *testing.T) {
	a := NewSilenceAnalyzer(30)
	const sr = 16000
	chunk := make([]byte, sr/50*2) // 20ms

	loud := make([]byte, len(chunk))
	for i := 0; i+1 < len(loud); i += 2 {
		binary.LittleEndian.PutUint16(loud[i:], 8000)
	}
	quiet := make([]byte, len(chunk))

	for range 3 {
		if s := a.AppendAudio(loud, sr, true); s != StateIncomplete {
			t.Fatalf("during speech: %v", s)
		}
	}
	for i := 0; i < 20; i++ {
		if a.AppendAudio(quiet, sr, false) == StateComplete {
			return
		}
	}
	t.Fatal("expected StateComplete after trailing silence")
}
