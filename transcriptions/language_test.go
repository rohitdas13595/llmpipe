package transcriptions

import (
	"testing"
)

func TestResolveLanguage_verified(t *testing.T) {
	m := map[Language]string{
		EN_US: "en-US-custom",
	}
	if got := ResolveLanguage(EN_US, m, true); got != "en-US-custom" {
		t.Fatalf("want verified map value, got %q", got)
	}
}

func TestResolveLanguage_baseFallback(t *testing.T) {
	m := map[Language]string{}
	if got := ResolveLanguage(EN_GB, m, true); got != "en" {
		t.Fatalf("want base en, got %q", got)
	}
}

func TestResolveLanguage_fullFallback(t *testing.T) {
	m := map[Language]string{}
	if got := ResolveLanguage(EN_GB, m, false); got != "en-GB" {
		t.Fatalf("want full tag, got %q", got)
	}
}
