package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohitdas13595/llmpipe/services/ttsbase"
)

func TestStreamingEndpoint(t *testing.T) {
	s := &Streaming{VoiceID: "voice", SampleRate: 16000, ModelID: DefaultStreamingModel}
	u := s.endpoint()
	for _, part := range []string{"/voice/multi-stream-input", "model_id=eleven_flash_v2_5", "sync_alignment=true", "auto_mode=true", "output_format=pcm_16000"} {
		if !strings.Contains(u, part) {
			t.Fatalf("endpoint %s missing %s", u, part)
		}
	}
}

func TestMultiStreamOrderedCloseAndAlignment(t *testing.T) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var mu sync.Mutex
	var clientMsgs []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("xi-api-key"); got != "test-key" {
			t.Errorf("key %q", got)
		}
		if !strings.Contains(r.URL.RawQuery, "sync_alignment=true") {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var msg map[string]any
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			mu.Lock()
			clientMsgs = append(clientMsgs, msg)
			mu.Unlock()
			text, _ := msg["text"].(string)
			id, _ := msg["context_id"].(string)
			if strings.TrimSpace(text) != "" {
				writeAudio(c, id, text)
			}
			if flush, _ := msg["flush"].(bool); flush {
				_ = c.WriteJSON(map[string]any{"isFinal": true, "contextId": id})
			}
			if closed, _ := msg["close_context"].(bool); closed {
				// Late audio for the closed context must be ignored by the client.
				writeAudio(c, id, "stale")
				writeAudio(c, "other", "kept")
			}
		}
	}))
	defer srv.Close()

	s := &Streaming{
		APIKey:     "test-key",
		VoiceID:    "voice",
		SampleRate: 16000,
		URL:        "ws" + strings.TrimPrefix(srv.URL, "http") + "/multi-stream-input?sync_alignment=true",
		KeepAlive:  time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SendText(ctx, "ctx-a", "Hello."); err != nil {
		t.Fatal(err)
	}
	if err := s.SendText(ctx, "ctx-b", "World."); err != nil {
		t.Fatal(err)
	}
	ev1 := readEvent(t, s.Receive())
	ev2 := readEvent(t, s.Receive())
	if ev1.ContextID != "ctx-a" || ev2.ContextID != "ctx-b" {
		t.Fatalf("order %s then %s", ev1.ContextID, ev2.ContextID)
	}
	if len(ev1.Audio.PCM) == 0 || ev1.Audio.Alignment == nil || len(ev1.Audio.Alignment.Chars) == 0 {
		t.Fatalf("audio/alignment %#v", ev1.Audio)
	}
	if ev1.Audio.Alignment.StartMS[0] != 0 || ev1.Audio.Alignment.DurMS[0] != 40 {
		t.Fatalf("alignment %+v", ev1.Audio.Alignment)
	}
	if err := s.Flush(ctx, "ctx-a"); err != nil {
		t.Fatal(err)
	}
	fin := readEvent(t, s.Receive())
	if !fin.Final || fin.ContextID != "ctx-a" {
		t.Fatalf("final %#v", fin)
	}
	if err := s.CloseContext(ctx, "ctx-a"); err != nil {
		t.Fatal(err)
	}
	kept := readEvent(t, s.Receive())
	if kept.ContextID != "other" {
		t.Fatalf("expected audio for the open context, got %#v", kept)
	}
	select {
	case ev := <-s.Receive():
		if ev.ContextID == "ctx-a" && len(ev.Audio.PCM) > 0 {
			t.Fatal("closed context audio was delivered")
		}
	case <-time.After(50 * time.Millisecond):
	}
	mu.Lock()
	defer mu.Unlock()
	var sawInit, sawFlush, sawClose bool
	for _, m := range clientMsgs {
		if m["text"] == " " && m["context_id"] == "ctx-a" {
			if _, ok := m["voice_settings"]; ok {
				sawInit = true
			}
		}
		if m["flush"] == true && m["context_id"] == "ctx-a" {
			sawFlush = true
		}
		if m["close_context"] == true && m["context_id"] == "ctx-a" {
			sawClose = true
		}
	}
	if !sawInit || !sawFlush || !sawClose {
		t.Fatalf("init %v flush %v close %v", sawInit, sawFlush, sawClose)
	}
}

func writeAudio(c *websocket.Conn, id, text string) {
	pcm := []byte{1, 2, 3, 4}
	chars := []string{}
	starts := []int{}
	durs := []int{}
	for i, r := range text {
		chars = append(chars, string(r))
		starts = append(starts, i*40)
		durs = append(durs, 40)
	}
	_ = c.WriteJSON(map[string]any{
		"audio":     base64.StdEncoding.EncodeToString(pcm),
		"contextId": id,
		"normalizedAlignment": map[string]any{
			"chars":            chars,
			"charStartTimesMs": starts,
			"charDurationsMs":  durs,
		},
	})
}

func readEvent(t *testing.T, ch <-chan ttsbase.StreamEvent) ttsbase.StreamEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("closed")
		}
		if ev.Err != nil {
			t.Fatal(ev.Err)
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
		return ttsbase.StreamEvent{}
	}
}
