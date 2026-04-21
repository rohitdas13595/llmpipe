package turn

// State is the end-of-utterance result (Pipecat: EndOfTurnState).
type State int

const (
	StateIncomplete State = iota
	StateComplete
)

// SilenceAnalyzer detects end of user turn using silence duration after speech
// (Pipecat: BaseSmartTurn.append_audio when ML path is unused — silence-only).
type SilenceAnalyzer struct {
	StopMS float64 // required silence after last speech chunk (e.g. 3000)

	speechSeen bool
	silenceMS  float64
}

// NewSilenceAnalyzer returns an analyzer with stopMS ms of trailing silence to mark COMPLETE.
// If stopMS <= 0, defaults to 3000 (matches Pipecat SmartTurnParams.STOP_SECS default intent).
func NewSilenceAnalyzer(stopMS float64) *SilenceAnalyzer {
	if stopMS <= 0 {
		stopMS = 3000
	}
	return &SilenceAnalyzer{StopMS: stopMS}
}

// AppendAudio updates state from one PCM s16le chunk. isSpeech should match your VAD/RMS gate.
func (s *SilenceAnalyzer) AppendAudio(pcm []byte, sampleRate int, isSpeech bool) State {
	if len(pcm) < 2 {
		return StateIncomplete
	}
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	chunkMS := float64(len(pcm)/2) / float64(sampleRate) * 1000

	if isSpeech {
		s.silenceMS = 0
		s.speechSeen = true
		return StateIncomplete
	}
	if s.speechSeen {
		s.silenceMS += chunkMS
		if s.silenceMS >= s.StopMS {
			s.speechSeen = false
			s.silenceMS = 0
			return StateComplete
		}
	}
	return StateIncomplete
}

// Reset clears state (e.g. new session).
func (s *SilenceAnalyzer) Reset() {
	s.speechSeen = false
	s.silenceMS = 0
}
