package frames

// TTSStartedFrame marks the start of a synthesized utterance. It travels in
// the data lane so it stays ordered with the audio it introduces.
type TTSStartedFrame struct {
	Control
	Meta
}

func (f *TTSStartedFrame) FrameKind() string { return KindTTSStarted }

// TTSStoppedFrame marks the end of a synthesized utterance, before playback
// has necessarily finished.
type TTSStoppedFrame struct {
	Control
	Meta
}

func (f *TTSStoppedFrame) FrameKind() string { return KindTTSStopped }

// TTSTextFrame is a slice of assistant text aligned to audio. PTS is the
// media time at which this slice has been fully heard. Output releases the
// frame downstream once playback reaches PTS. It is a system frame so a
// following InterruptionFrame cannot overtake text that was already played.
type TTSTextFrame struct {
	Text      string
	ContextID string
	System
	Meta
}

func (f *TTSTextFrame) FrameKind() string { return KindTTSText }
