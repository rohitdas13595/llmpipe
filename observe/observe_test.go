package observe

import (
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
)

func TestFuncObserver(t *testing.T) {
	var n int
	f := FuncObserver{F: func(p FramePushed) { n++ }}
	f.OnFrame(FramePushed{Frame: &frames.TextFrame{}})
	if n != 1 {
		t.Fatalf("n = %d", n)
	}
}

func TestIdleFrameObserverFiresWithoutReset(t *testing.T) {
	done := make(chan struct{})
	cfg := IdleConfig{
		Timeout:    60 * time.Millisecond,
		ResetKinds: map[string]struct{}{},
		OnIdle: func() { close(done) },
	}
	i := NewIdleFrameObserver(cfg)
	i.Start()
	defer i.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnIdle not called")
	}
}
