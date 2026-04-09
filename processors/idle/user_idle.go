// Package idle provides user idle detection.
package idle

import (
	"context"
	"sync"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// UserCallback is invoked when the user has been silent for Timeout after conversation activity.
type UserCallback func(retryCount int) (continueMonitoring bool)

// UserProcessor resets a timer on user/bot activity and runs Callback on timeout.
type UserProcessor struct {
	name       string
	Timeout    time.Duration
	Callback   UserCallback
	mu         sync.Mutex
	timer      *time.Timer
	retryCount int
	started    bool
}

func NewUserProcessor(name string, timeout time.Duration, cb UserCallback) *UserProcessor {
	return &UserProcessor{name: name, Timeout: timeout, Callback: cb}
}

func (u *UserProcessor) Name() string { return u.name }

func (u *UserProcessor) armTimer() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.Timeout <= 0 || u.Callback == nil {
		return
	}
	if u.timer != nil {
		if !u.timer.Stop() {
			select {
			case <-u.timer.C:
			default:
			}
		}
	}
	u.timer = time.AfterFunc(u.Timeout, func() {
		u.mu.Lock()
		rc := u.retryCount
		u.mu.Unlock()
		cont := u.Callback(rc)
		u.mu.Lock()
		if cont {
			u.retryCount++
		} else {
			u.retryCount = 0
		}
		u.mu.Unlock()
		if cont {
			u.armTimer()
		}
	})
}

func (u *UserProcessor) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	switch f.(type) {
	case *frames.StartFrame:
		u.started = true
		// Arm inactivity timer from session start so silent clients (no VAD / STT frames) still time out.
		u.armTimer()
	case *frames.UserStartedSpeakingFrame, *frames.TranscriptionFrame, *frames.BotStoppedSpeakingFrame:
		if u.started {
			u.mu.Lock()
			u.retryCount = 0
			u.mu.Unlock()
			u.armTimer()
		}
	}
	emit.Down(f)
	return nil
}
