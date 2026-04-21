package local

import (
	"bytes"
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

func TestLocalOutputWrites(t *testing.T) {
	var out bytes.Buffer
	q := func(_ context.Context, fs []frames.Frame) error { return nil }
	tr := NewTransport(8000, nil, &out, q)
	o := tr.Output()
	emit := processor.Emit{Down: func(frames.Frame) {}, Up: func(frames.Frame) {}}
	if err := o.Process(context.Background(), &frames.TTSAudioRawFrame{Audio: []byte{1, 2, 3}}, processor.Downstream, emit); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 3 {
		t.Fatalf("out len %d", out.Len())
	}
}
