//go:build integration

package elevenlabs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
)

func TestStreamingIntegrationFirstAudio(t *testing.T) {
	key := os.Getenv("ELEVENLABS_API_KEY")
	voice := os.Getenv("ELEVENLABS_VOICE_ID")
	if key == "" || voice == "" {
		t.Skip("ELEVENLABS_API_KEY or ELEVENLABS_VOICE_ID is not set")
	}
	tts := NewStreamingTTS("tts", key, voice, nil, 16000)
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(tts, rec.Processor("rec")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !task.Running() {
		if time.Now().After(deadline) {
			t.Fatal("not running")
		}
		time.Sleep(time.Millisecond)
	}
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.LLMFullResponseStartFrame{},
		&frames.LLMTextFrame{Text: "Hello from the streaming test."},
		&frames.LLMFullResponseEndFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		for _, f := range rec.Frames() {
			if fr, ok := f.(*frames.ErrorFrame); ok {
				t.Fatal(fr.Err)
			}
			if _, ok := f.(*frames.TTSAudioRawFrame); ok {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("stop")
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no audio")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
