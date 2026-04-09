package observe

import (
	"sync"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
)

// IdleConfig configures pipeline session idle (frame observer + task idle).
type IdleConfig struct {
	Timeout      time.Duration
	ResetKinds   map[string]struct{}
	OnIdle       func()
	CancelOnIdle bool
}

// IdleFrameObserver watches frame kinds and triggers OnIdle after Timeout.
type IdleFrameObserver struct {
	cfg    IdleConfig
	resetC chan struct{}
	stopC  chan struct{}
	once   sync.Once
}

// NewIdleFrameObserver creates an idle watchdog. Start must be called.
func NewIdleFrameObserver(cfg IdleConfig) *IdleFrameObserver {
	if cfg.ResetKinds == nil {
		cfg.ResetKinds = DefaultIdleResetKinds()
	}
	return &IdleFrameObserver{
		cfg:    cfg,
		resetC: make(chan struct{}, 1),
		stopC:  make(chan struct{}),
	}
}

// Start begins the watchdog goroutine.
func (i *IdleFrameObserver) Start() {
	i.once.Do(func() {
		if i.cfg.Timeout <= 0 {
			return
		}
		go i.run()
	})
}

func (i *IdleFrameObserver) run() {
	timer := time.NewTimer(i.cfg.Timeout)
	defer timer.Stop()

	for {
		select {
		case <-i.stopC:
			return
		case <-i.resetC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(i.cfg.Timeout)
		case <-timer.C:
			if i.cfg.OnIdle != nil {
				i.cfg.OnIdle()
			}
			return
		}
	}
}

// Stop stops the watchdog.
func (i *IdleFrameObserver) Stop() {
	select {
	case <-i.stopC:
	default:
		close(i.stopC)
	}
}

// OnFrame implements FrameObserver — resets timer when a reset-kind frame appears.
func (i *IdleFrameObserver) OnFrame(p FramePushed) {
	if i.cfg.Timeout <= 0 {
		return
	}
	k := ""
	if p.Frame != nil {
		k = p.Frame.FrameKind()
	}
	if _, ok := i.cfg.ResetKinds[k]; !ok {
		return
	}
	select {
	case i.resetC <- struct{}{}:
	default:
	}
}

var _ FrameObserver = (*IdleFrameObserver)(nil)

// DefaultIdleResetKinds matches common idle-timeout reset activity.
func DefaultIdleResetKinds() map[string]struct{} {
	return map[string]struct{}{
		frames.KindBotSpeaking:         {},
		frames.KindUserStartedSpeaking: {},
		frames.KindBotStartedSpeaking:  {},
	}
}
