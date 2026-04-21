// Package base holds shared transport configuration types aligned with Pipecat base_transport.TransportParams.
package base

// Params configures audio/video behavior for transports (subset of Pipecat TransportParams).
type Params struct {
	AudioOutEnabled       bool
	AudioOutSampleRate    int
	AudioOutChannels      int
	AudioOutBitrate       int
	AudioOut10msChunks    int
	AudioOutEndSilenceSec int
	AudioOutAutoSilence   bool

	AudioInEnabled       bool
	AudioInSampleRate    int
	AudioInChannels      int
	AudioInStreamOnStart bool
	AudioInPassthrough   bool

	VideoInEnabled  bool
	VideoOutEnabled bool
	VideoOutWidth   int
	VideoOutHeight  int
	VideoOutBitrate int
	VideoOutFPS     int
}

// DefaultParams returns Pipecat-like defaults for PCM voice agents.
func DefaultParams() Params {
	return Params{
		AudioOut10msChunks:    4,
		AudioOutBitrate:       96000,
		AudioOutEndSilenceSec: 2,
		AudioOutAutoSilence:   true,
		AudioInChannels:       1,
		AudioInStreamOnStart:  true,
		AudioInPassthrough:    true,
		VideoOutWidth:         1024,
		VideoOutHeight:        768,
		VideoOutBitrate:       800000,
		VideoOutFPS:           30,
	}
}
