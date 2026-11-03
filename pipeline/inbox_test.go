package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestInboxSystemBeforeData(t *testing.T) {
	in := newInbox(8, 10)
	ctx := context.Background()
	if err := in.push(ctx, queued{frame: &frames.TextFrame{Text: "d"}, dir: processor.Downstream}); err != nil {
		t.Fatal(err)
	}
	if err := in.push(ctx, queued{frame: &frames.CancelFrame{}, dir: processor.Downstream}); err != nil {
		t.Fatal(err)
	}
	first, ok := in.pop(ctx)
	if !ok {
		t.Fatal("pop")
	}
	if _, ok := first.frame.(*frames.CancelFrame); !ok {
		t.Fatalf("first = %T", first.frame)
	}
	second, ok := in.pop(ctx)
	if !ok {
		t.Fatal("pop2")
	}
	if _, ok := second.frame.(*frames.TextFrame); !ok {
		t.Fatalf("second = %T", second.frame)
	}
}

func TestInboxUpstreamBeforeDownstreamData(t *testing.T) {
	in := newInbox(8, 10)
	ctx := context.Background()
	_ = in.push(ctx, queued{frame: &frames.TextFrame{Text: "down"}, dir: processor.Downstream})
	_ = in.push(ctx, queued{frame: &frames.TextFrame{Text: "up"}, dir: processor.Upstream})
	got, ok := in.pop(ctx)
	if !ok || got.dir != processor.Upstream {
		t.Fatalf("got dir %v ok %v", got.dir, ok)
	}
}

func TestInboxDrainKeepsEndAndResults(t *testing.T) {
	in := newInbox(8, 10)
	ctx := context.Background()
	_ = in.push(ctx, queued{frame: &frames.TextFrame{Text: "a"}, dir: processor.Downstream})
	_ = in.push(ctx, queued{frame: &frames.EndFrame{}, dir: processor.Downstream})
	_ = in.push(ctx, queued{frame: &frames.FunctionCallResultFrame{Result: "ok"}, dir: processor.Downstream})
	_ = in.push(ctx, queued{frame: &frames.LLMTextFrame{Text: "x"}, dir: processor.Upstream})
	if n := in.drainInterruptible(); n != 2 {
		t.Fatalf("dropped %d", n)
	}
	var kinds []string
	for len(kinds) < 2 {
		q, ok := in.pop(context.Background())
		if !ok {
			t.Fatal("missing kept frame")
		}
		kinds = append(kinds, q.frame.FrameKind())
	}
	if len(kinds) != 2 || kinds[0] != frames.KindEnd || kinds[1] != frames.KindFunctionCallResult {
		t.Fatalf("kinds %v", kinds)
	}
}

func TestInboxDownstreamBackpressure(t *testing.T) {
	in := newInbox(1, 10)
	ctx := context.Background()
	if err := in.push(ctx, queued{frame: &frames.TextFrame{Text: "1"}, dir: processor.Downstream}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- in.push(ctx, queued{frame: &frames.TextFrame{Text: "2"}, dir: processor.Downstream})
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("push did not block: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, ok := in.pop(ctx); !ok {
		t.Fatal("pop")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pusher stuck")
	}
	hi, _ := in.stats()
	if hi > 1 {
		t.Fatalf("hi-water %d", hi)
	}
}

func TestInboxPushCtxCancel(t *testing.T) {
	in := newInbox(1, 10)
	_ = in.push(context.Background(), queued{frame: &frames.TextFrame{Text: "1"}, dir: processor.Downstream})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := in.push(ctx, queued{frame: &frames.TextFrame{Text: "2"}, dir: processor.Downstream})
	if err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestInboxAudioDropOldest(t *testing.T) {
	in := newInbox(8, 3)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		f := &frames.InputAudioRawFrame{Audio: []byte{byte(i)}}
		if err := in.push(ctx, queued{frame: f, dir: processor.Downstream}); err != nil {
			t.Fatal(err)
		}
	}
	_, drops := in.stats()
	if drops != 7 {
		t.Fatalf("drops %d", drops)
	}
	var got []byte
	for len(got) < 3 {
		q, ok := in.pop(ctx)
		if !ok {
			t.Fatal("missing audio")
		}
		a := q.frame.(*frames.InputAudioRawFrame)
		got = append(got, a.Audio[0])
	}
	if len(got) != 3 || got[0] != 7 || got[1] != 8 || got[2] != 9 {
		t.Fatalf("remaining %v", got)
	}
}

func TestInboxCloseUnblocks(t *testing.T) {
	in := newInbox(1, 4)
	done := make(chan struct{})
	go func() {
		_, _ = in.pop(context.Background())
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	in.close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pop did not unblock")
	}
}
