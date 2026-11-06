//go:build integration

package deepgram

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/pipeline/pipelinetest"
)

func TestLiveIntegrationConnect(t *testing.T) {
	key := os.Getenv("DEEPGRAM_API_KEY")
	if key == "" {
		t.Skip("DEEPGRAM_API_KEY is not set")
	}
	live := NewLive("stt", key, nil, 16000, nil)
	rec := pipelinetest.NewRecorder()
	task := pipeline.NewPipelineTask(pipeline.NewPipeline(live, rec.Processor("rec")))
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
	pcm := make([]byte, 16000*2/5) // 200 ms of silence
	if err := task.QueueFrames(context.Background(), []frames.Frame{
		&frames.UserStartedSpeakingFrame{},
		&frames.InputAudioRawFrame{Audio: pcm, SampleRate: 16000, NumChannels: 1},
		&frames.UserStoppedSpeakingFrame{},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	for _, f := range rec.Frames() {
		if fr, ok := f.(*frames.ErrorFrame); ok {
			t.Fatal(fr.Err)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop")
	}
}
