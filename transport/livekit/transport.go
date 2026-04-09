// Package livekit provides Input/Output FrameProcessors for a LiveKit room.
package livekit

import (
	"context"
	"encoding/binary"
	"log"
	"sync"

	"github.com/livekit/media-sdk"
	protoLogger "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// Transport connects as a bot participant, subscribes to the first remote Opus microphone,
// and publishes bot audio from TTS PCM.
type Transport struct {
	URL    string
	Info   lksdk.ConnectInfo
	Logger protoLogger.Logger

	SampleRate  int
	queue       func(context.Context, []frames.Frame) error
	connectOpts []lksdk.ConnectOption
	// OnDisconnect is called when a remote participant leaves or the room session ends unexpectedly.
	// Not invoked for disconnects initiated by Transport.Disconnect (intentional agent shutdown).
	OnDisconnect func()

	mu       sync.RWMutex
	room     *lksdk.Room
	localPCM *lkmedia.PCMLocalTrack
	subOnce  sync.Once
	intentMu sync.Mutex
	closing  bool // true while Transport.Disconnect is tearing down the room
}

// NewTransport builds a LiveKit transport. queue should inject at pipeline start (e.g. task.QueueFrames).
// Optional connectOpts are passed to ConnectToRoom (e.g. ConnectOptionsFromEnv()).
func NewTransport(url string, info lksdk.ConnectInfo, sampleRate int, queue func(context.Context, []frames.Frame) error, log protoLogger.Logger, connectOpts ...lksdk.ConnectOption) *Transport {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	if log == nil {
		log = protoLogger.GetLogger()
	}
	return &Transport{URL: url, Info: info, SampleRate: sampleRate, queue: queue, Logger: log, connectOpts: append([]lksdk.ConnectOption(nil), connectOpts...)}
}

// Connect joins the room, publishes a PCM local track, and wires the first subscribed mic to queue.
func (t *Transport) Connect(ctx context.Context) error {
	cb := &lksdk.RoomCallback{
		OnParticipantDisconnected: t.onRemoteParticipantDisconnected,
		OnDisconnected:            t.onRoomDisconnected,
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackSubscribed: t.onTrackSubscribed,
		},
	}
	opts := append([]lksdk.ConnectOption(nil), t.connectOpts...)
	if len(opts) == 0 {
		opts = []lksdk.ConnectOption{lksdk.WithAutoSubscribe(true)}
	}
	room, err := lksdk.ConnectToRoom(t.URL, t.Info, cb, opts...)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.room = room
	t.mu.Unlock()

	lt, err := lkmedia.NewPCMLocalTrack(t.SampleRate, 1, t.Logger)
	if err != nil {
		room.Disconnect()
		return err
	}
	if _, err = room.LocalParticipant.PublishTrack(lt, &lksdk.TrackPublicationOptions{Name: "agent"}); err != nil {
		lt.Close()
		room.Disconnect()
		return err
	}
	t.mu.Lock()
	t.localPCM = lt
	t.mu.Unlock()
	return nil
}

// Disconnect leaves the room and closes the published track.
func (t *Transport) Disconnect() {
	t.intentMu.Lock()
	t.closing = true
	t.intentMu.Unlock()

	t.mu.Lock()
	room := t.room
	lt := t.localPCM
	t.room = nil
	t.localPCM = nil
	t.mu.Unlock()
	if lt != nil {
		lt.ClearQueue()
		lt.Close()
	}
	if room != nil {
		room.Disconnect()
	}

	t.intentMu.Lock()
	t.closing = false
	t.intentMu.Unlock()
}

func (t *Transport) onRemoteParticipantDisconnected(rp *lksdk.RemoteParticipant) {
	if rp == nil {
		return
	}
	t.mu.RLock()
	room := t.room
	t.mu.RUnlock()
	if room == nil {
		return
	}
	if rp.Identity() == room.LocalParticipant.Identity() {
		return
	}
	t.fireOnDisconnect()
}

func (t *Transport) onRoomDisconnected() {
	t.intentMu.Lock()
	closing := t.closing
	t.intentMu.Unlock()
	if closing {
		return
	}
	t.fireOnDisconnect()
}

func (t *Transport) fireOnDisconnect() {
	t.mu.RLock()
	fn := t.OnDisconnect
	t.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

func (t *Transport) onTrackSubscribed(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	if track.Kind() != webrtc.RTPCodecTypeAudio || track.Codec().MimeType != webrtc.MimeTypeOpus {
		return
	}
	t.mu.RLock()
	room := t.room
	t.mu.RUnlock()
	if room == nil || rp.Identity() == room.LocalParticipant.Identity() {
		return
	}
	if t.queue == nil {
		return
	}
	t.subOnce.Do(func() {
		w := &pcmQueueWriter{q: t.queue, sampleRate: t.SampleRate}
		_, err := lkmedia.NewPCMRemoteTrack(track, w, lkmedia.WithTargetSampleRate(t.SampleRate))
		if err != nil {
			t.Logger.Errorw("livekit pcm remote track", err)
			return
		}
		log.Printf("llmpipe: livekit — remote Opus mic wired into pipeline (participant %q)", rp.Identity())
	})
}

type pcmQueueWriter struct {
	q          func(context.Context, []frames.Frame) error
	sampleRate int
}

func (w *pcmQueueWriter) WriteSample(sample media.PCM16Sample) error {
	if len(sample) == 0 {
		return nil
	}
	buf := make([]byte, 2*len(sample))
	for i, v := range sample {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(v))
	}
	return w.q(context.Background(), []frames.Frame{
		&frames.InputAudioRawFrame{Audio: buf, SampleRate: w.sampleRate, NumChannels: 1},
	})
}

func (w *pcmQueueWriter) Close() error { return nil }

// Input is a passthrough; audio is queued from the LiveKit subscription callback.
func (t *Transport) Input() processor.Processor {
	return processor.Func{
		N: "livekit.input",
		F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
			emit.Down(f)
			return nil
		},
	}
}

// Output writes TTSAudioRawFrame to the published PCM track.
func (t *Transport) Output() processor.Processor {
	return &lkOutput{t: t}
}

type lkOutput struct {
	t *Transport
}

func (o *lkOutput) Name() string { return "livekit.output" }

func (o *lkOutput) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	if a, ok := f.(*frames.TTSAudioRawFrame); ok {
		o.t.mu.RLock()
		lt := o.t.localPCM
		o.t.mu.RUnlock()
		if lt != nil && len(a.Audio) >= 2 {
			n := len(a.Audio) / 2
			samples := make(media.PCM16Sample, n)
			for i := 0; i < n; i++ {
				samples[i] = int16(binary.LittleEndian.Uint16(a.Audio[i*2:]))
			}
			_ = lt.WriteSample(samples)
		}
	}
	emit.Down(f)
	return nil
}
