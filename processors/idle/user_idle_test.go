package idle

import (
	"context"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestUserProcessorInvokesCallbackAfterTimeout(t *testing.T) {
	done := make(chan int, 1)
	u := NewUserProcessor("u", 50*time.Millisecond, func(retry int) bool {
		done <- retry
		return false
	})
	emit := processor.Emit{Down: func(frames.Frame) {}}
	if err := u.Process(context.Background(), &frames.StartFrame{}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if err := u.Process(context.Background(), &frames.UserStartedSpeakingFrame{}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r != 0 {
			t.Fatalf("retry = %d", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not invoked")
	}
}

func TestUserProcessorNoCallbackBeforeStartFrame(t *testing.T) {
	called := make(chan struct{})
	u := NewUserProcessor("u", 30*time.Millisecond, func(retry int) bool {
		close(called)
		return false
	})
	emit := processor.Emit{Down: func(frames.Frame) {}}
	_ = u.Process(context.Background(), &frames.UserStartedSpeakingFrame{}, processor.Downstream, emit)
	select {
	case <-called:
		t.Fatal("should not arm before StartFrame")
	case <-time.After(100 * time.Millisecond):
	}
}
