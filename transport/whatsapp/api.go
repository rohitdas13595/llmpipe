// Package whatsapp implements the WhatsApp Cloud API calls facet (Graph API).
// WebRTC signalling is left to your app; Pipecat pairs this with smallwebrtc.
package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const graphAPIVersion = "https://graph.facebook.com/v23.0"

// API is a WhatsApp Calling API client (Pipecat WhatsAppApi).
type API struct {
	Token         string
	PhoneNumberID string
	HTTP          *http.Client
}

// NewAPI creates a client for POST /{phone-number-id}/calls.
func NewAPI(token, phoneNumberID string, httpClient *http.Client) *API {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &API{Token: token, PhoneNumberID: phoneNumberID, HTTP: httpClient}
}

func (a *API) callsURL() string {
	return fmt.Sprintf("%s/%s/calls", graphAPIVersion, a.PhoneNumberID)
}

// AnswerCall sends pre_accept or accept with an SDP answer (Pipecat answer_call_to_whatsapp).
func (a *API) AnswerCall(ctx context.Context, callID, action, sdp, from string) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"to":                from,
		"action":            action,
		"call_id":           callID,
		"session":           map[string]string{"sdp": sdp, "sdp_type": "answer"},
	})
	return a.postCalls(ctx, body)
}

// RejectCall rejects an incoming call.
func (a *API) RejectCall(ctx context.Context, callID string) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"action":            "reject",
		"call_id":           callID,
	})
	return a.postCalls(ctx, body)
}

// TerminateCall ends an active call.
func (a *API) TerminateCall(ctx context.Context, callID string) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"action":            "terminate",
		"call_id":           callID,
	})
	return a.postCalls(ctx, body)
}

func (a *API) postCalls(ctx context.Context, payload []byte) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.callsURL(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("whatsapp api: %s: %s", resp.Status, string(raw))
	}
	return raw, nil
}
