package deepgram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/sttbase"
)

const (
	defaultLiveModel = "nova-3"
	preRoll          = 500 * time.Millisecond
	keepAliveEvery   = 5 * time.Second
)

// Live is Deepgram's streaming recognition socket.
// Batch recognition stays available as NewSTT (STT=deepgram_batch).
// Audio is forwarded while the user is speaking, with preroll so the start
// of the utterance is not clipped. Finalize is sent when speech stops.
type Live struct {
	name       string
	APIKey     string
	Model      string
	SampleRate int
	Reenter    services.ReenterFunc
	Bot        *aggregate.BotState
	// MuteWhileBotSpeaking drops mic audio while the bot is playing.
	MuteWhileBotSpeaking bool
	// URL overrides the production listen endpoint (tests).
	URL string
	// VADGated, when true (the default), sends audio only during speech
	// plus preroll. Set it false to stream continuously.
	VADGated bool
	// keepEvery overrides the 5s keepalive. Tests set a short interval.
	keepEvery time.Duration

	services.Slot

	mu         sync.Mutex
	session    *sttbase.Session
	preroll    []byte
	prerollMax int
	speaking   bool
	vadGated   bool
}

// NewLive builds a streaming Deepgram STT. sampleRate is PCM Hz (16 kHz when <= 0).
func NewLive(name, apiKey string, reenter services.ReenterFunc, sampleRate int, bot *aggregate.BotState) *Live {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	return &Live{
		name:       name,
		APIKey:     apiKey,
		Model:      defaultLiveModel,
		SampleRate: sampleRate,
		Reenter:    reenter,
		Bot:        bot,
		vadGated:   true,
		VADGated:   true,
		prerollMax: int(preRoll.Seconds() * float64(sampleRate) * 2),
	}
}

func (l *Live) Name() string { return l.name }

func (l *Live) dropEcho() bool {
	return l.MuteWhileBotSpeaking && l.Bot != nil && l.Bot.Speaking()
}

// Setup dials Deepgram and starts the reconnect loop.
func (l *Live) Setup(ctx context.Context, h processor.Handle) error {
	if err := l.Slot.Setup(ctx, h); err != nil {
		return err
	}
	if strings.TrimSpace(l.APIKey) == "" && strings.TrimSpace(l.URL) == "" {
		return nil
	}
	l.vadGated = l.VADGated
	if l.prerollMax <= 0 {
		sr := l.SampleRate
		if sr <= 0 {
			sr = 16000
		}
		l.prerollMax = sr * 2 / 2
	}
	every := l.keepEvery
	if every <= 0 {
		every = keepAliveEvery
	}
	l.session = sttbase.New(sttbase.Config{
		Dial:             l.dial,
		KeepAliveEvery:   every,
		KeepAlivePayload: []byte(`{"type":"KeepAlive"}`),
		RingBytes:        l.SampleRate * 2 * 2,
		MaxFailures:      5,
		OnMessage:        l.onMessage,
		OnPermanentError: func(err error) {
			_ = l.push(l.ProcContext(), &frames.ErrorFrame{Err: err})
		},
	})
	l.session.Start(h.Context())
	return nil
}

// Cleanup closes the websocket.
func (l *Live) Cleanup(ctx context.Context) error {
	if l.session != nil {
		return l.session.Close()
	}
	return nil
}

func (l *Live) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if dir == processor.Upstream {
		processor.Forward(dir, f, emit)
		return nil
	}
	switch fr := f.(type) {
	case *frames.StartFrame:
		if fr.SampleRate > 0 {
			l.SampleRate = fr.SampleRate
		}
		emit.Down(f)
	case *frames.UserStartedSpeakingFrame:
		l.mu.Lock()
		l.speaking = true
		pre := append([]byte(nil), l.preroll...)
		l.preroll = l.preroll[:0]
		l.mu.Unlock()
		if l.session != nil && !l.dropEcho() && len(pre) > 0 {
			_ = l.session.WriteAudio(pre)
		}
		emit.Down(f)
	case *frames.InputAudioRawFrame:
		l.takeAudio(fr.Audio)
		emit.Down(f)
	case *frames.UserStoppedSpeakingFrame:
		l.mu.Lock()
		l.speaking = false
		l.mu.Unlock()
		l.finalize()
		emit.Down(f)
	case *frames.EndFrame:
		if l.session != nil {
			_ = l.session.WriteText([]byte(`{"type":"CloseStream"}`))
		}
		emit.Down(f)
	default:
		emit.Down(f)
	}
	return nil
}

func (l *Live) takeAudio(pcm []byte) {
	if len(pcm) == 0 || l.dropEcho() {
		return
	}
	l.mu.Lock()
	speaking := l.speaking
	gated := l.vadGated
	if gated && !speaking {
		l.preroll = append(l.preroll, pcm...)
		if extra := len(l.preroll) - l.prerollMax; extra > 0 {
			if extra%2 == 1 {
				extra++
			}
			if extra > len(l.preroll) {
				extra = len(l.preroll)
			}
			l.preroll = append([]byte(nil), l.preroll[extra:]...)
		}
	}
	sess := l.session
	l.mu.Unlock()
	if sess != nil && (!gated || speaking) {
		_ = sess.WriteAudio(pcm)
	}
}

func (l *Live) finalize() {
	if l.dropEcho() {
		return
	}
	if l.session == nil {
		if strings.TrimSpace(l.APIKey) == "" {
			_ = l.push(l.ProcContext(), &frames.ErrorFrame{Err: errMissingKey})
		}
		return
	}
	_ = l.session.WriteText([]byte(`{"type":"Finalize"}`))
}

func (l *Live) push(ctx context.Context, f frames.Frame) error {
	return services.PushDown(ctx, l.H(), l.Reenter, l.name, f)
}

func (l *Live) onMessage(mt int, data []byte) {
	if mt != websocket.TextMessage {
		return
	}
	fr := parseLive(data)
	if fr == nil {
		return
	}
	_ = l.push(l.ProcContext(), fr)
}

func (l *Live) endpoint() string {
	if strings.TrimSpace(l.URL) != "" {
		return l.URL
	}
	q := url.Values{}
	model := l.Model
	if model == "" {
		model = defaultLiveModel
	}
	sr := l.SampleRate
	if sr <= 0 {
		sr = 16000
	}
	q.Set("model", model)
	q.Set("encoding", "linear16")
	q.Set("sample_rate", strconv.Itoa(sr))
	q.Set("channels", "1")
	q.Set("interim_results", "true")
	q.Set("endpointing", "300")
	q.Set("utterance_end_ms", "1000")
	q.Set("vad_events", "true")
	q.Set("smart_format", "true")
	return "wss://api.deepgram.com/v1/listen?" + q.Encode()
}

func (l *Live) dial(ctx context.Context) (sttbase.Conn, error) {
	header := http.Header{}
	if l.APIKey != "" {
		header.Set("Authorization", "Token "+l.APIKey)
	}
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Proxy: http.ProxyFromEnvironment}
	conn, _, err := d.DialContext(ctx, l.endpoint(), header)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type errString string

func (e errString) Error() string { return string(e) }

const errMissingKey errString = "deepgram: missing API key"

type dgAlt struct {
	Transcript string   `json:"transcript"`
	Languages  []string `json:"languages"`
}

type dgLive struct {
	Type        string `json:"type"`
	IsFinal     bool   `json:"is_final"`
	SpeechFinal bool   `json:"speech_final"`
	Channel     struct {
		Alternatives []dgAlt `json:"alternatives"`
	} `json:"channel"`
}

// parseLive maps a Deepgram websocket message to a transcript frame.
// Results with is_final become TranscriptionFrame. Earlier hypotheses become
// InterimTranscriptionFrame. SpeechStarted and UtteranceEnd carry no text.
func parseLive(data []byte) frames.Frame {
	var msg dgLive
	if json.Unmarshal(data, &msg) != nil {
		return nil
	}
	if !strings.EqualFold(msg.Type, "Results") {
		return nil
	}
	text := ""
	lang := ""
	if len(msg.Channel.Alternatives) > 0 {
		text = strings.TrimSpace(msg.Channel.Alternatives[0].Transcript)
		if len(msg.Channel.Alternatives[0].Languages) > 0 {
			lang = msg.Channel.Alternatives[0].Languages[0]
		}
	}
	if text == "" {
		return nil
	}
	if !msg.IsFinal {
		return &frames.InterimTranscriptionFrame{Text: text}
	}
	return &frames.TranscriptionFrame{
		Text:      text,
		Final:     true,
		Language:  lang,
		Timestamp: time.Now(),
	}
}
