// Package smallwebrtc provides a thin helper around github.com/pion/webrtc for Pipecat-style ICE setup.
// Build your PeerConnection, negotiate SDP, and bridge tracks to llmpipe frames in application code.
package smallwebrtc

import (
	"github.com/pion/webrtc/v4"
)

// NewPeerConnection creates a PeerConnection with STUN (and optional TURN) ICE servers.
// urls entries are passed as single-URL ICEServer entries (e.g. "stun:stun.l.google.com:19302").
func NewPeerConnection(iceURLs []string) (*webrtc.PeerConnection, error) {
	servers := make([]webrtc.ICEServer, 0, len(iceURLs))
	for _, u := range iceURLs {
		if u == "" {
			continue
		}
		servers = append(servers, webrtc.ICEServer{URLs: []string{u}})
	}
	cfg := webrtc.Configuration{ICEServers: servers}
	return webrtc.NewPeerConnection(cfg)
}
