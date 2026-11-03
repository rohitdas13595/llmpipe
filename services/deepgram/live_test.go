package deepgram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
)

func TestParseLiveInterimAndFinal(t *testing.T) {
	interim := parseLive([]byte(`{"type":"Results","is_final":false,"channel":{"alternatives":[{"transcript":"hel"}]}}`))
	if _, ok := interim.(*frames.InterimTranscriptionFrame); !ok {
		t.Fatalf("interim %T", interim)
	}
	final := parseLive([]byte(`{"type":"Results","is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"hello","languages":["en"]}]}}`))
	tf, ok := final.(*frames.TranscriptionFrame)
	if !ok || tf.Text != "hello" || !tf.Final || tf.Language != "en" || tf.Timestamp.IsZero() {
		t.Fatalf("final %#v", final)
	}
	if parseLive([]byte(`{"type":"SpeechStarted"}`)) != nil {
		t.Fatal("speech started should not be a transcript")
	}
}

func TestLiveMappingFinalizeAndKeepAlive(t *testing.T) {
	var mu sync.Mutex
	var texts []string
	gotAudio := make(chan struct{}, 1)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Token test-key" {
			t.Errorf("auth %q", got)
		}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"Results","is_final":false,"channel":{"alternatives":[{"transcript":"hel"}]}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"Results","is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"hello","languages":["en"]}]}}`))
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			mt, msg, err := c.ReadMessage()
			if err != nil {
				continue
			}
			if mt == websocket.BinaryMessage {
				select {
				case gotAudio <- struct{}{}:
				default:
				}
				continue
			}
			mu.Lock()
			texts = append(texts, string(msg))
			mu.Unlock()
		}
	}))
	defer srv.Close()

	live := NewLive("stt", "test-key", nil, 16000, nil)
	live.URL = "ws" + strings.TrimPrefix(srv.URL, "http")
	live.keepEvery = 25 * time.Millisecond
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(live, rec.Processor("rec")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("not running")
		}
		time.Sleep(time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	var sawInterim, sawFinal bool
	for !sawInterim || !sawFinal {
		for _, f := range rec.Frames() {
			if fr, ok := f.(*frames.InterimTranscriptionFrame); ok && fr.Text == "hel" {
				sawInterim = true
			}
			if fr, ok := f.(*frames.TranscriptionFrame); ok && fr.Text == "hello" && fr.Final && fr.Language == "en" {
				sawFinal = true
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("interim %v final %v", sawInterim, sawFinal)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.UserStartedSpeakingFrame{},
		&frames.InputAudioRawFrame{Audio: []byte{1, 2, 3, 4}, SampleRate: 16000},
		&frames.UserStoppedSpeakingFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		joined := strings.Join(texts, "\n")
		mu.Unlock()
		if strings.Contains(joined, `"Finalize"`) && strings.Contains(joined, `"KeepAlive"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages %s", joined)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-gotAudio:
	case <-time.After(time.Second):
		t.Fatal("server did not receive audio")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop")
	}
}

func TestLiveEndpointDefaults(t *testing.T) {
	l := NewLive("stt", "k", nil, 16000, nil)
	u := l.endpoint()
	for _, part := range []string{"model=nova-3", "interim_results=true", "endpointing=300", "utterance_end_ms=1000", "vad_events=true", "smart_format=true", "encoding=linear16"} {
		if !strings.Contains(u, part) {
			t.Fatalf("endpoint %s missing %s", u, part)
		}
	}
}

func TestLiveJSONControlShape(t *testing.T) {
	var msg map[string]string
	if json.Unmarshal([]byte(`{"type":"Finalize"}`), &msg) != nil || msg["type"] != "Finalize" {
		t.Fatal(msg)
	}
}
