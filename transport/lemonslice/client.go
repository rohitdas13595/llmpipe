// Package lemonslice implements the LemonSlice session HTTP API (Pipecat: lemonslice/api.py).
// Sessions target Daily unless you swap transport_type in a custom fork.
package lemonslice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const defaultSessionsURL = "https://lemonslice.com/api/liveai/sessions"

// Client is the LemonSlice HTTP client.
type Client struct {
	APIKey  string
	BaseURL string
	HTTP    *http.Client
}

// NewClient builds a client. baseURL empty uses the Pipecat default endpoint.
func NewClient(apiKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{APIKey: apiKey, BaseURL: defaultSessionsURL, HTTP: httpClient}
}

// CreateSessionRequest mirrors Pipecat create_session kwargs.
type CreateSessionRequest struct {
	AgentImageURL   string         `json:"agent_image_url,omitempty"`
	AgentID         string         `json:"agent_id,omitempty"`
	AgentPrompt     string         `json:"agent_prompt,omitempty"`
	IdleTimeout     *int           `json:"idle_timeout,omitempty"`
	DailyRoomURL    string         `json:"-"`
	DailyToken      string         `json:"-"`
	ConnectionProps map[string]any `json:"-"`
	Extra           map[string]any `json:"-"`
	APIURL          string         `json:"-"`
	TransportType   string         `json:"transport_type,omitempty"` // default "daily"
}

// CreateSessionResponse is the JSON returned by LemonSlice (keys vary; we keep common ones).
type CreateSessionResponse struct {
	SessionID  string          `json:"session_id"`
	RoomURL    string          `json:"room_url"`
	ControlURL string          `json:"control_url"`
	Raw        json.RawMessage `json:"-"`
}

// CreateSession POSTs a new avatar session.
func (c *Client) CreateSession(ctx context.Context, req CreateSessionRequest) (CreateSessionResponse, error) {
	if req.AgentID == "" && req.AgentImageURL == "" {
		return CreateSessionResponse{}, fmt.Errorf("lemonslice: agent_id or agent_image_url required")
	}
	if req.AgentID != "" && req.AgentImageURL != "" {
		return CreateSessionResponse{}, fmt.Errorf("lemonslice: provide only one of agent_id or agent_image_url")
	}
	tt := req.TransportType
	if tt == "" {
		tt = "daily"
	}
	payload := map[string]any{"transport_type": tt}
	if req.AgentID != "" {
		payload["agent_id"] = req.AgentID
	}
	if req.AgentImageURL != "" {
		payload["agent_image_url"] = req.AgentImageURL
	}
	if req.AgentPrompt != "" {
		payload["agent_prompt"] = req.AgentPrompt
	}
	if req.IdleTimeout != nil {
		payload["idle_timeout"] = *req.IdleTimeout
	}
	props := map[string]any{}
	for k, v := range req.ConnectionProps {
		props[k] = v
	}
	if req.DailyRoomURL != "" {
		props["daily_url"] = req.DailyRoomURL
	}
	if req.DailyToken != "" {
		props["daily_token"] = req.DailyToken
	}
	if len(props) > 0 {
		payload["properties"] = props
	}
	for k, v := range req.Extra {
		payload[k] = v
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return CreateSessionResponse{}, err
	}
	url := c.BaseURL
	if req.APIURL != "" {
		url = req.APIURL
	}
	if strings.TrimSpace(url) == "" {
		url = defaultSessionsURL
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return CreateSessionResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.APIKey)
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return CreateSessionResponse{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CreateSessionResponse{}, fmt.Errorf("lemonslice: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var out CreateSessionResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return CreateSessionResponse{}, err
	}
	out.Raw = raw
	return out, nil
}

// EndSession POSTs terminate to control_url (Pipecat end_session).
func EndSession(ctx context.Context, c *http.Client, controlURL, apiKey string) error {
	if c == nil {
		c = http.DefaultClient
	}
	body := strings.NewReader(`{"event":"terminate"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("lemonslice end: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}
