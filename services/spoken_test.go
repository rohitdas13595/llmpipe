package services

import (
	"strings"
	"testing"
	"time"
)

func TestTTSTextFramesSentencePTS(t *testing.T) {
	// 1s of 16 kHz mono s16le.
	pcmBytes := 16000 * 2
	frames := TTSTextFrames("Hello there. And more.", pcmBytes, 16000)
	if len(frames) != 2 {
		t.Fatalf("sentences = %d (%v)", len(frames), frames)
	}
	if frames[0].Text != "Hello there." || frames[1].Text != "And more." {
		t.Fatalf("text = %q / %q", frames[0].Text, frames[1].Text)
	}
	if frames[0].PTS <= 0 || frames[0].PTS >= time.Second {
		t.Fatalf("first PTS = %s", frames[0].PTS)
	}
	if frames[1].PTS != time.Second {
		t.Fatalf("last PTS = %s, want 1s", frames[1].PTS)
	}
	if strings.TrimSpace(frames[0].Text) == "" {
		t.Fatal("empty")
	}
}
