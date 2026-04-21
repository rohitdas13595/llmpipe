// Package serializers encodes llmpipe frames for wire transfer (Pipecat-compatible protobuf + JSON).
package serializers

import (
	"errors"
	"fmt"

	"github.com/rohitdas13595/llmpipe/frames"
)

// Serializer converts frames to bytes and back (Pipecat: FrameSerializer).
type Serializer interface {
	Serialize(f frames.Frame) ([]byte, error)
	Deserialize(b []byte) (frames.Frame, error)
}

var (
	// ErrUnsupportedFrame means Serialize does not handle this concrete frame type.
	ErrUnsupportedFrame = errors.New("serializers: unsupported frame type")
	// ErrDecode indicates malformed input to Deserialize.
	ErrDecode = errors.New("serializers: decode error")
)

func unsupportedType(f frames.Frame) error {
	if f == nil {
		return fmt.Errorf("%w: nil frame", ErrUnsupportedFrame)
	}
	return fmt.Errorf("%w: %T", ErrUnsupportedFrame, f)
}
