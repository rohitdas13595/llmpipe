package whatsapp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// VerifyWebhookChallenge validates the GET verification handshake (Pipecat handle_verify_webhook_request).
// Returns the hub.challenge string to echo on success.
func VerifyWebhookChallenge(params map[string]string, expectedVerifyToken string) (string, error) {
	mode := params["hub.mode"]
	challenge := params["hub.challenge"]
	verifyToken := params["hub.verify_token"]
	if mode == "" || challenge == "" || verifyToken == "" {
		return "", fmt.Errorf("whatsapp webhook: missing hub.mode, hub.challenge, or hub.verify_token")
	}
	if mode != "subscribe" {
		return "", fmt.Errorf("whatsapp webhook: invalid hub.mode %q", mode)
	}
	if verifyToken != expectedVerifyToken {
		return "", fmt.Errorf("whatsapp webhook: verify_token mismatch")
	}
	return challenge, nil
}

// VerifyWebhookChallengeInt returns hub.challenge as int (Pipecat returns int for FastAPI).
func VerifyWebhookChallengeInt(params map[string]string, expectedVerifyToken string) (int, error) {
	s, err := VerifyWebhookChallenge(params, expectedVerifyToken)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(s)
}

// ValidateSignature verifies X-Hub-Signature-256 (sha256=...) against the app secret.
func ValidateSignature(appSecret string, rawBody []byte, signatureHeader string) error {
	if appSecret == "" {
		return fmt.Errorf("whatsapp webhook: empty app secret")
	}
	if signatureHeader == "" {
		return fmt.Errorf("whatsapp webhook: missing X-Hub-Signature-256")
	}
	received := strings.TrimPrefix(signatureHeader, "sha256=")
	mac := hmac.New(sha256.New, []byte(appSecret))
	_, _ = mac.Write(rawBody)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(received)) {
		return fmt.Errorf("whatsapp webhook: invalid signature")
	}
	return nil
}
