package whatsapp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestVerifyWebhookChallenge(t *testing.T) {
	params := map[string]string{
		"hub.mode":         "subscribe",
		"hub.challenge":    "1158201444",
		"hub.verify_token": "secret",
	}
	n, err := VerifyWebhookChallengeInt(params, "secret")
	if err != nil || n != 1158201444 {
		t.Fatalf("got %d err %v", n, err)
	}
}

func TestValidateSignature(t *testing.T) {
	secret := "appsecret"
	body := []byte(`{"object":"test"}`)
	if err := ValidateSignature(secret, body, ""); err == nil {
		t.Fatal("expected error for empty header")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if err := ValidateSignature(secret, body, sig); err != nil {
		t.Fatal(err)
	}
}
