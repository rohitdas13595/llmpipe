// Package examples_test contains godoc Example_* snippets for common llmpipe APIs.
// Run: go test -v -run '^Example' ./examples
package examples_test

import (
	"fmt"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/serializers"
	"github.com/rohitdas13595/llmpipe/transcriptions"
	"github.com/rohitdas13595/llmpipe/transport/whatsapp"
)

func ExampleResolveLanguage_verified() {
	m := map[transcriptions.Language]string{
		transcriptions.EN_US: "en-US-provider",
	}
	fmt.Println(transcriptions.ResolveLanguage(transcriptions.EN_US, m, true))
	// Output: en-US-provider
}

func ExampleResolveLanguage_fallbackBase() {
	m := map[transcriptions.Language]string{}
	fmt.Println(transcriptions.ResolveLanguage(transcriptions.EN_GB, m, true))
	// Output: en
}

func ExampleJSON_roundTripText() {
	var j serializers.JSON
	f := &frames.TextFrame{Text: "ping"}
	b, err := j.Serialize(f)
	if err != nil {
		fmt.Println(err)
		return
	}
	out, err := j.Deserialize(b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(out.(*frames.TextFrame).Text)
	// Output: ping
}

func ExampleProtobuf_roundTripText() {
	var s serializers.Protobuf
	f := &frames.TextFrame{Text: "hello"}
	b, err := s.Serialize(f)
	if err != nil {
		fmt.Println(err)
		return
	}
	out, err := s.Deserialize(b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(out.(*frames.TextFrame).Text)
	// Output: hello
}

func ExampleVerifyWebhookChallengeInt() {
	params := map[string]string{
		"hub.mode":         "subscribe",
		"hub.challenge":    "123456",
		"hub.verify_token": "mytoken",
	}
	n, err := whatsapp.VerifyWebhookChallengeInt(params, "mytoken")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(n)
	// Output: 123456
}

func ExampleTransportMessageFrame() {
	f := &frames.TransportMessageFrame{Data: `{"type":"ping"}`}
	fmt.Println(f.Data)
	// Output: {"type":"ping"}
}
