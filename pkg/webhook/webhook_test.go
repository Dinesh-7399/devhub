package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestCalculateSignature_Stripe(t *testing.T) {
	secret := "whsec_testsecret123"
	payload := `{"id":"evt_123","type":"payment_intent.succeeded"}`
	ts := int64(1700000000)

	key, val, err := CalculateSignature(ProviderStripe, secret, payload, ts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if key != "Stripe-Signature" {
		t.Errorf("expected Stripe-Signature header, got %s", key)
	}

	// Verify HMAC manually
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.%s", ts, payload)))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	expectedVal := fmt.Sprintf("t=%d,v1=%s", ts, expectedSig)

	if val != expectedVal {
		t.Errorf("expected signature %s, got %s", expectedVal, val)
	}
}

func TestCalculateSignature_GitHub(t *testing.T) {
	secret := "github_secret_999"
	payload := `{"action":"opened","issue":{"number":1}}`

	key, val, err := CalculateSignature(ProviderGitHub, secret, payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if key != "X-Hub-Signature-256" {
		t.Errorf("expected X-Hub-Signature-256 header, got %s", key)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expectedSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if val != expectedSig {
		t.Errorf("expected signature %s, got %s", expectedSig, val)
	}
}

func TestCalculateSignature_Razorpay(t *testing.T) {
	secret := "rzp_secret_key"
	payload := `{"event":"payment.authorized"}`

	key, val, err := CalculateSignature(ProviderRazorpay, secret, payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if key != "X-Razorpay-Signature" {
		t.Errorf("expected X-Razorpay-Signature, got %s", key)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	if val != expectedSig {
		t.Errorf("expected signature %s, got %s", expectedSig, val)
	}
}

func TestBypassHeaders(t *testing.T) {
	headers := map[string]string{
		"Stripe-Signature": "t=123,v1=oldbadhash",
		"Content-Type":     "application/json",
		"User-Agent":       "Stripe/1.0",
	}

	bypassed := BypassHeaders(headers)

	if _, exists := bypassed["Stripe-Signature"]; exists {
		t.Errorf("Stripe-Signature should have been stripped")
	}
	if bypassed["Content-Type"] != "application/json" {
		t.Errorf("Content-Type was dropped")
	}
	if bypassed["X-DevHub-Test-Mode"] != "true" {
		t.Errorf("expected X-DevHub-Test-Mode: true")
	}
}

func TestDetectProvider(t *testing.T) {
	if DetectProvider(map[string]string{"Stripe-Signature": "..."}) != ProviderStripe {
		t.Errorf("failed to detect stripe")
	}
	if DetectProvider(map[string]string{"X-Hub-Signature-256": "..."}) != ProviderGitHub {
		t.Errorf("failed to detect github")
	}
	if DetectProvider(map[string]string{"X-Razorpay-Signature": "..."}) != ProviderRazorpay {
		t.Errorf("failed to detect razorpay")
	}
}
