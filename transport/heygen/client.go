// Package heygen implements the HeyGen Interactive Avatar REST API (streaming.new / start / stop).
// Realtime WebSocket + LiveKit media require separate wiring (see Pipecat heygen client).
package heygen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// HeyGen sample rate for streamed audio (Pipecat HEY_GEN_SAMPLE_RATE).
const AudioSampleRate = 24000

const apiBaseURL = "https://api.heygen.com/v1"

// Client is the HeyGen interactive streaming API client.
type Client struct {
	APIKey  string
	BaseURL string
	HTTP    *http.Client
}

// NewClient creates a client using api.heygen.com/v1.
func NewClient(apiKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{APIKey: apiKey, BaseURL: apiBaseURL, HTTP: httpClient}
}

func (c *Client) base() string {
	if strings.TrimSpace(c.BaseURL) == "" {
		return apiBaseURL
	}
	return strings.TrimSuffix(c.BaseURL, "/")
}

// NewInteractiveSessionRequest is a minimal streaming.new payload.
type NewInteractiveSessionRequest struct {
	Quality             string         `json:"quality,omitempty"`
	AvatarID            string         `json:"avatar_id"`
	Version             string         `json:"version,omitempty"`
	VideoEncoding       string         `json:"video_encoding,omitempty"`
	KnowledgeID         string         `json:"knowledge_id,omitempty"`
	KnowledgeBase       string         `json:"knowledge_base,omitempty"`
	DisableIdleTimeout  *bool          `json:"disable_idle_timeout,omitempty"`
	ActivityIdleTimeout *int           `json:"activity_idle_timeout,omitempty"`
	Voice               map[string]any `json:"voice,omitempty"`
}

// Session bundles URLs and tokens returned after new + start.
type Session struct {
	SessionID         string
	AccessToken       string
	LivekitAgentToken string
	RealtimeEndpoint  string `json:"realtime_endpoint"`
	LivekitURL        string `json:"url"`
	RawNew            json.RawMessage
}

type envelope struct {
	Data json.RawMessage `json:"data"`
}

type sessionNewData struct {
	SessionID         string `json:"session_id"`
	AccessToken       string `json:"access_token"`
	LivekitAgentToken string `json:"livekit_agent_token"`
	RealtimeEndpoint  string `json:"realtime_endpoint"`
	URL               string `json:"url"`
}

// NewInteractiveSession calls POST /streaming.new then /streaming.start.
func (c *Client) NewInteractiveSession(ctx context.Context, req NewInteractiveSessionRequest) (Session, error) {
	if req.Version == "" {
		req.Version = "v2"
	}
	params := map[string]any{
		"quality":               req.Quality,
		"avatar_id":             req.AvatarID,
		"voice":                 req.Voice,
		"knowledge_id":          req.KnowledgeID,
		"knowledge_base":        req.KnowledgeBase,
		"version":               req.Version,
		"video_encoding":        req.VideoEncoding,
		"disable_idle_timeout":  req.DisableIdleTimeout,
		"activity_idle_timeout": req.ActivityIdleTimeout,
	}
	body, _ := json.Marshal(params)
	b, err := c.post(ctx, "/streaming.new", body, true)
	if err != nil {
		return Session{}, err
	}
	var info sessionNewData
	if err := json.Unmarshal(b, &info); err != nil {
		return Session{}, err
	}
	s := Session{
		SessionID:         info.SessionID,
		AccessToken:       info.AccessToken,
		LivekitAgentToken: info.LivekitAgentToken,
		RealtimeEndpoint:  info.RealtimeEndpoint,
		LivekitURL:        info.URL,
		RawNew:            b,
	}
	startBody, _ := json.Marshal(map[string]string{"session_id": s.SessionID})
	if _, err := c.post(ctx, "/streaming.start", startBody, true); err != nil {
		return Session{}, err
	}
	return s, nil
}

// CloseSession calls POST /streaming.stop.
func (c *Client) CloseSession(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return fmt.Errorf("heygen: empty session_id")
	}
	body, _ := json.Marshal(map[string]string{"session_id": sessionID})
	_, err := c.post(ctx, "/streaming.stop", body, false)
	return err
}

func (c *Client) post(ctx context.Context, path string, jsonBody []byte, expectJSON bool) ([]byte, error) {
	url := c.base() + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.APIKey)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("heygen %s: %s: %s", path, resp.Status, strings.TrimSpace(string(raw)))
	}
	if !expectJSON {
		return raw, nil
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return []byte("{}"), nil
	}
	return env.Data, nil
}
