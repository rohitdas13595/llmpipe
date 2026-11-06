package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

type fakeSock struct {
	msgs []map[string]any
}

func (f *fakeSock) WriteJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	f.msgs = append(f.msgs, m)
	return nil
}

func (f *fakeSock) ReadMessage() (int, []byte, error) { return 0, nil, io.EOF }
func (f *fakeSock) Close() error                      { return nil }

func TestRealtimeCancelAndTruncate(t *testing.T) {
	sock := &fakeSock{}
	r := NewRealtime("rt", "key", "model", "", nil, nil)
	r.conn = sock
	delta := base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4})
	raw := []byte(`{"type":"response.output_audio.delta","delta":"` + delta + `","item_id":"item_1"}`)
	r.handleServer(context.Background(), raw)
	if r.itemID != "item_1" {
		t.Fatalf("item id %q", r.itemID)
	}
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) {}}
	if err := r.Process(context.Background(), &frames.InterruptionFrame{}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if err := r.Process(context.Background(), &frames.BotStoppedSpeakingFrame{Interrupted: true, PlayedMS: 320}, processor.Upstream, emit); err != nil {
		t.Fatal(err)
	}
	var cancel, trunc bool
	for _, m := range sock.msgs {
		switch m["type"] {
		case "response.cancel":
			cancel = true
		case "conversation.item.truncate":
			trunc = true
			if m["item_id"] != "item_1" {
				t.Fatalf("item_id %#v", m["item_id"])
			}
			if m["audio_end_ms"] != float64(320) {
				t.Fatalf("audio_end_ms %#v", m["audio_end_ms"])
			}
		}
	}
	if !cancel || !trunc {
		t.Fatalf("msgs = %#v", sock.msgs)
	}
}
