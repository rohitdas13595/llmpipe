package serializers

import (
	"bytes"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
)

func TestProtobufTextAndAudio(t *testing.T) {
	var p Protobuf
	tf := &frames.TextFrame{Text: "hi"}
	b, err := p.Serialize(tf)
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.Deserialize(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.(*frames.TextFrame).Text != "hi" {
		t.Fatalf("text roundtrip")
	}

	audio := &frames.InputAudioRawFrame{Audio: []byte{1, 0, 2, 0}, SampleRate: 16000, NumChannels: 1}
	b, err = p.Serialize(audio)
	if err != nil {
		t.Fatal(err)
	}
	out, err = p.Deserialize(b)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*frames.InputAudioRawFrame)
	if !bytes.Equal(audio.Audio, got.Audio) || audio.SampleRate != got.SampleRate || audio.NumChannels != got.NumChannels {
		t.Fatalf("input audio mismatch: %+v", got)
	}

	tts := &frames.TTSAudioRawFrame{Audio: []byte{9, 9}, SampleRate: 24000, NumChannels: 1}
	b, err = p.Serialize(tts)
	if err != nil {
		t.Fatal(err)
	}
	out, err = p.Deserialize(b)
	if err != nil {
		t.Fatal(err)
	}
	gotIn := out.(*frames.InputAudioRawFrame)
	if !bytes.Equal(tts.Audio, gotIn.Audio) || tts.SampleRate != int(gotIn.SampleRate) {
		t.Fatalf("tts serializes as audio; deserialize is InputAudioRawFrame per Pipecat")
	}
}

func TestProtobufTransportMessage(t *testing.T) {
	var p Protobuf
	f := &frames.TransportMessageFrame{Data: `{"m":1}`}
	b, err := p.Serialize(f)
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.Deserialize(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.(*frames.TransportMessageFrame).Data != `{"m":1}` {
		t.Fatal("transport message")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	var j JSON
	cases := []frames.Frame{
		&frames.TextFrame{Text: "a"},
		&frames.TurnStartedFrame{Index: 3},
		&frames.TurnEndedFrame{Index: 2, Complete: false},
	}
	for _, f := range cases {
		b, err := j.Serialize(f)
		if err != nil {
			t.Fatalf("serialize %T: %v", f, err)
		}
		out, err := j.Deserialize(b)
		if err != nil {
			t.Fatalf("deserialize: %v", err)
		}
		if out.FrameKind() != f.FrameKind() {
			t.Fatalf("kind %s vs %s", out.FrameKind(), f.FrameKind())
		}
	}
}

func TestErrUnsupported(t *testing.T) {
	var p Protobuf
	if _, err := p.Serialize(&frames.StartFrame{}); err == nil {
		t.Fatal("expected unsupported")
	}
	var j JSON
	if _, err := j.Serialize(&frames.StartFrame{}); err == nil {
		t.Fatal("expected unsupported")
	}
}
