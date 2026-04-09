package livekit

import (
	"os"
	"strings"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
)

// ConnectOptionsFromEnv returns LiveKit SDK connect options tuned for real networks.
// The server-sdk default per-attempt timeout is 3s, which often fails on high-latency
// routes or when LiveKit Cloud tries several regional edges in sequence.
func ConnectOptionsFromEnv() []lksdk.ConnectOption {
	opts := []lksdk.ConnectOption{lksdk.WithAutoSubscribe(true)}

	timeout := 30 * time.Second
	if s := strings.TrimSpace(os.Getenv("LIVEKIT_CONNECT_TIMEOUT")); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			timeout = d
		}
	}
	opts = append(opts, lksdk.WithConnectTimeout(timeout))

	switch v := strings.TrimSpace(strings.ToLower(os.Getenv("LIVEKIT_DISABLE_REGION_DISCOVERY"))); v {
	case "1", "true", "yes", "on":
		opts = append(opts, lksdk.WithDisableRegionDiscovery())
	}

	switch strings.ToLower(strings.TrimSpace(os.Getenv("LIVEKIT_ICE_TRANSPORT"))) {
	case "relay":
		opts = append(opts, lksdk.WithICETransportPolicy(webrtc.ICETransportPolicyRelay))
	}

	return opts
}
