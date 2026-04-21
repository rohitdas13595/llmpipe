// Package tavus implements the Tavus REST API (v2). Pipecat wires media through Daily rooms;
// use transport/livekit or transport/websocket/client with ConversationURL when you have a WebRTC/WebSocket URL from Tavus.
package tavus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const defaultBaseURL = "https://tavusapi.com/v2"

// Dev constants matching Pipecat (sample room / mock persona).
const (
	MockConversationID = "dev-conversation"
	MockPersonaName    = "TestTavusTransport"
)

// Client calls Tavus HTTP endpoints.
type Client struct {
	APIKey     string
	BaseURL    string
	HTTP       *http.Client
	devRoomURL string // TAVUS_SAMPLE_ROOM_URL
}

// NewClient creates a Tavus API client.
func NewClient(apiKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		APIKey:     apiKey,
		BaseURL:    strings.TrimSuffix(defaultBaseURL, "/"),
		HTTP:       httpClient,
		devRoomURL: os.Getenv("TAVUS_SAMPLE_ROOM_URL"),
	}
}

// CreateConversationResponse matches POST /conversations.
type CreateConversationResponse struct {
	ConversationID  string `json:"conversation_id"`
	ConversationURL string `json:"conversation_url"`
	RoomURL         string `json:"room_url"` // some responses use room_url
	raw             json.RawMessage
}

// Raw returns the full JSON object for fields not modeled here.
func (r CreateConversationResponse) Raw() json.RawMessage { return r.raw }

// CreateConversation starts a conversation (replica + persona).
func (c *Client) CreateConversation(ctx context.Context, replicaID, personaID string) (CreateConversationResponse, error) {
	if c.devRoomURL != "" {
		return CreateConversationResponse{
				ConversationID:  MockConversationID,
				ConversationURL: c.devRoomURL,
				raw:             nil,
			},
			nil
	}
	body, _ := json.Marshal(map[string]string{
		"replica_id": replicaID,
		"persona_id": personaID,
	})
	return c.postConversation(ctx, body)
}

func (c *Client) postConversation(ctx context.Context, payload []byte) (CreateConversationResponse, error) {
	url := c.base() + "/conversations"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return CreateConversationResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.APIKey)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return CreateConversationResponse{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CreateConversationResponse{}, fmt.Errorf("tavus: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out CreateConversationResponse
	if err := json.Unmarshal(b, &out); err != nil {
		return CreateConversationResponse{}, err
	}
	out.raw = b
	if out.ConversationURL == "" && out.RoomURL != "" {
		out.ConversationURL = out.RoomURL
	}
	return out, nil
}

// EndConversation POST /conversations/{id}/end
func (c *Client) EndConversation(ctx context.Context, conversationID string) error {
	if conversationID == "" || conversationID == MockConversationID {
		return nil
	}
	url := fmt.Sprintf("%s/conversations/%s/end", c.base(), conversationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.APIKey)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("tavus end: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (c *Client) base() string {
	if c.BaseURL != "" {
		return strings.TrimSuffix(c.BaseURL, "/")
	}
	return strings.TrimSuffix(defaultBaseURL, "/")
}

// Persona holds GET /personas/{id} subset.
type Persona struct {
	PersonaName string `json:"persona_name"`
}

// GetPersona fetches persona metadata.
func (c *Client) GetPersona(ctx context.Context, personaID string) (Persona, error) {
	if c.devRoomURL != "" {
		return Persona{PersonaName: MockPersonaName}, nil
	}
	url := fmt.Sprintf("%s/personas/%s", c.base(), personaID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Persona{}, err
	}
	req.Header.Set("x-api-key", c.APIKey)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Persona{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Persona{}, fmt.Errorf("tavus persona: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var p Persona
	if err := json.Unmarshal(b, &p); err != nil {
		return Persona{}, err
	}
	return p, nil
}
