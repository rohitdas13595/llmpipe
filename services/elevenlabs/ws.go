package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/services/ttsbase"
)

// DefaultStreamingModel is ElevenLabs Flash, used for the multi-stream socket.
const DefaultStreamingModel = "eleven_flash_v2_5"

// Streaming is the ElevenLabs multi-stream-input websocket.
// Field names were checked against the current multi-stream API:
// client messages use context_id, flush, close_context, close_socket, and text;
// server messages use audio, isFinal, contextId, and alignment / normalizedAlignment
// with chars, charStartTimesMs, and charDurationsMs.
type Streaming struct {
	APIKey     string
	VoiceID    string
	ModelID    string
	SampleRate int
	// URL replaces the production websocket URL. Tests point this at httptest.
	URL string
	// KeepAlive refreshes open contexts. Zero means 15s.
	KeepAlive time.Duration

	mu      sync.Mutex
	wmu     sync.Mutex
	conn    *websocket.Conn
	events  chan ttsbase.StreamEvent
	opened  map[string]bool
	closed  map[string]bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	done    bool
}

// NewStreamingTTS is the default ElevenLabs processor: sentence aggregation
// over one multi-stream websocket, with a context id per LLM turn.
func NewStreamingTTS(name, apiKey, voiceID string, bot *aggregate.BotState, sampleRate int) *ttsbase.TTS {
	_ = bot
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	s := &Streaming{
		APIKey:     apiKey,
		VoiceID:    voiceID,
		ModelID:    DefaultStreamingModel,
		SampleRate: sampleRate,
	}
	return ttsbase.NewStreaming(s, ttsbase.Options{Name: name, SampleRate: sampleRate, PushTextFrames: true})
}

// Connect dials the multi-stream socket. A second call is a no-op.
func (s *Streaming) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	if strings.TrimSpace(s.VoiceID) == "" && s.URL == "" {
		s.mu.Unlock()
		return fmt.Errorf("elevenlabs: missing voice id")
	}
	if s.SampleRate <= 0 {
		s.SampleRate = 16000
	}
	if s.ModelID == "" {
		s.ModelID = DefaultStreamingModel
	}
	if s.KeepAlive <= 0 {
		s.KeepAlive = 15 * time.Second
	}
	s.events = make(chan ttsbase.StreamEvent, 32)
	s.opened = map[string]bool{}
	s.closed = map[string]bool{}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true
	s.mu.Unlock()

	if err := s.dial(s.ctx); err != nil {
		s.fail(err)
		return err
	}
	s.mu.Lock()
	if s.done {
		conn := s.conn
		s.conn = nil
		s.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		return nil
	}
	s.wg.Add(2)
	s.mu.Unlock()
	go s.readLoop()
	go s.keepLoop()
	return nil
}

// SendText initializes the context on first use, then appends text.
// The socket requires a trailing space, so one is added when missing.
func (s *Streaming) SendText(ctx context.Context, contextID, text string) error {
	if strings.TrimSpace(contextID) == "" {
		return fmt.Errorf("elevenlabs: empty context id")
	}
	s.mu.Lock()
	if s.closed[contextID] {
		s.mu.Unlock()
		return fmt.Errorf("elevenlabs: context %s is closed", contextID)
	}
	needInit := !s.opened[contextID]
	if needInit {
		s.opened[contextID] = true
	}
	s.mu.Unlock()
	if needInit {
		if err := s.write(ctx, map[string]any{
			"text":       " ",
			"context_id": contextID,
			"voice_settings": map[string]any{
				"stability":        0.5,
				"similarity_boost": 0.75,
				"speed":            1.0,
			},
		}); err != nil {
			return err
		}
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if !strings.HasSuffix(text, " ") {
		text += " "
	}
	return s.write(ctx, map[string]any{"text": text, "context_id": contextID})
}

// Flush asks the server to synthesize whatever is buffered for contextID.
func (s *Streaming) Flush(ctx context.Context, contextID string) error {
	return s.write(ctx, map[string]any{"context_id": contextID, "flush": true})
}

// CloseContext drops later audio for contextID and tells the server to stop it.
func (s *Streaming) CloseContext(ctx context.Context, contextID string) error {
	s.mu.Lock()
	if s.closed == nil {
		s.closed = map[string]bool{}
	}
	s.closed[contextID] = true
	s.mu.Unlock()
	return s.write(ctx, map[string]any{"context_id": contextID, "close_context": true})
}

// Receive yields audio, finals, and errors. It is closed after Close.
func (s *Streaming) Receive() <-chan ttsbase.StreamEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

// Close sends close_socket and waits for the read loop.
func (s *Streaming) Close() error {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return nil
	}
	s.done = true
	cancel := s.cancel
	conn := s.conn
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		body, _ := json.Marshal(map[string]any{"close_socket": true})
		s.wmu.Lock()
		_ = conn.WriteMessage(websocket.TextMessage, body)
		s.wmu.Unlock()
		_ = conn.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Streaming) endpoint() string {
	if strings.TrimSpace(s.URL) != "" {
		return s.URL
	}
	q := url.Values{}
	q.Set("model_id", s.ModelID)
	q.Set("output_format", pcmOutputFormat(s.SampleRate))
	q.Set("sync_alignment", "true")
	// auto_mode skips the character schedule so the first sentence is spoken
	// without waiting for a 120-character buffer.
	q.Set("auto_mode", "true")
	return "wss://api.elevenlabs.io/v1/text-to-speech/" + url.PathEscape(s.VoiceID) + "/multi-stream-input?" + q.Encode()
}

func (s *Streaming) dial(ctx context.Context) error {
	header := http.Header{}
	if s.APIKey != "" {
		header.Set("xi-api-key", s.APIKey)
	}
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Proxy: http.ProxyFromEnvironment}
	conn, _, err := d.DialContext(ctx, s.endpoint(), header)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	return nil
}

func (s *Streaming) write(ctx context.Context, v any) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("elevenlabs: websocket is not connected")
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, body)
}

func (s *Streaming) readLoop() {
	defer s.wg.Done()
	defer s.closeEvents()
	for {
		s.mu.Lock()
		conn := s.conn
		s.mu.Unlock()
		if conn == nil {
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			if s.ctx != nil && s.ctx.Err() == nil && !s.isDone() {
				s.emit(ttsbase.StreamEvent{Err: err})
			}
			return
		}
		ev, ok := parseServer(data)
		if !ok {
			continue
		}
		s.mu.Lock()
		drop := s.closed[ev.ContextID]
		s.mu.Unlock()
		if drop {
			continue
		}
		s.emit(ev)
	}
}

func (s *Streaming) keepLoop() {
	defer s.wg.Done()
	tick := time.NewTicker(s.KeepAlive)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
			s.mu.Lock()
			var ids []string
			for id, open := range s.opened {
				if open && !s.closed[id] {
					ids = append(ids, id)
				}
			}
			s.mu.Unlock()
			for _, id := range ids {
				_ = s.write(s.ctx, map[string]any{"text": "", "context_id": id})
			}
		}
	}
}

func (s *Streaming) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *Streaming) emit(ev ttsbase.StreamEvent) {
	if s.ctx == nil {
		return
	}
	select {
	case <-s.ctx.Done():
	case s.events <- ev:
	}
}

func (s *Streaming) closeEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events != nil {
		close(s.events)
		s.events = nil
	}
}

func (s *Streaming) fail(err error) {
	s.mu.Lock()
	if s.events != nil {
		select {
		case s.events <- ttsbase.StreamEvent{Err: err}:
		default:
		}
		close(s.events)
		s.events = nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
}

type wireAlign struct {
	Chars   []string `json:"chars"`
	StartMS []int    `json:"charStartTimesMs"`
	DurMS   []int    `json:"charDurationsMs"`
}

type wireServer struct {
	Audio      string     `json:"audio"`
	IsFinal    bool       `json:"isFinal"`
	ContextID  string     `json:"contextId"`
	Alignment  *wireAlign `json:"alignment"`
	Normalized *wireAlign `json:"normalizedAlignment"`
}

func parseServer(data []byte) (ttsbase.StreamEvent, bool) {
	var msg wireServer
	if json.Unmarshal(data, &msg) != nil {
		return ttsbase.StreamEvent{}, false
	}
	if msg.Audio == "" && !msg.IsFinal {
		return ttsbase.StreamEvent{}, false
	}
	ev := ttsbase.StreamEvent{ContextID: msg.ContextID, Final: msg.IsFinal}
	if msg.Audio != "" {
		pcm, err := base64.StdEncoding.DecodeString(msg.Audio)
		if err != nil {
			return ttsbase.StreamEvent{Err: fmt.Errorf("elevenlabs: audio: %w", err), ContextID: msg.ContextID}, true
		}
		ev.Audio.PCM = pcm
		ev.Audio.Alignment = preferAlign(msg.Normalized, msg.Alignment)
	} else {
		ev.Audio.Alignment = preferAlign(msg.Normalized, msg.Alignment)
	}
	return ev, true
}

func preferAlign(norm, raw *wireAlign) *ttsbase.Alignment {
	w := norm
	if w == nil || len(w.Chars) == 0 {
		w = raw
	}
	if w == nil || len(w.Chars) == 0 {
		return nil
	}
	return &ttsbase.Alignment{Chars: append([]string(nil), w.Chars...), StartMS: append([]int(nil), w.StartMS...), DurMS: append([]int(nil), w.DurMS...)}
}
