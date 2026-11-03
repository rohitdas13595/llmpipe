package frames

// System marks a high-priority frame. System frames are dequeued ahead of
// data and are not removed by an interruption drain. Unmarked frames are data.
type System struct{}

func (System) isSystem() {}

// Control marks a pipeline control frame (for example End or LLMRun).
// Control frames travel in the data lane so an End queued behind audio is
// processed after that audio, and they are dropped on interruption unless
// they also embed Uninterruptible.
type Control struct{}

func (Control) isControl() {}

// Uninterruptible marks a frame that survives InterruptionFrame draining
// (End, function-call results).
type Uninterruptible struct{}

func (Uninterruptible) isUninterruptible() {}

type systemMark interface{ isSystem() }
type controlMark interface{ isControl() }
type uninterruptibleMark interface{ isUninterruptible() }

// IsSystem reports whether f is in the system category.
func IsSystem(f Frame) bool {
	_, ok := f.(systemMark)
	return ok
}

// IsControl reports whether f is in the control category.
func IsControl(f Frame) bool {
	_, ok := f.(controlMark)
	return ok
}

// IsUninterruptible reports whether an interruption drain must keep f.
func IsUninterruptible(f Frame) bool {
	_, ok := f.(uninterruptibleMark)
	return ok
}

// Interruptible reports whether an InterruptionFrame should drop f.
// System frames and uninterruptible frames are kept. Nil is not interruptible.
func Interruptible(f Frame) bool {
	return f != nil && !IsUninterruptible(f) && !IsSystem(f)
}
