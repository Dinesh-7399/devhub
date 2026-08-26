package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Supported webhook providers
const (
	ProviderStripe   = "stripe"
	ProviderGitHub   = "github"
	ProviderRazorpay = "razorpay"
	ProviderShopify  = "shopify"
	ProviderSlack    = "slack"
	ProviderGeneric  = "generic_sha256"
)

// CalculateSignature computes the provider-specific HMAC signature for a webhook payload.
func CalculateSignature(provider string, secret string, payload string, customTimestamp int64) (headerKey string, headerVal string, err error) {
	ts := customTimestamp
	if ts <= 0 {
		ts = time.Now().Unix()
	}

	switch strings.ToLower(provider) {
	case ProviderStripe:
		// Stripe format: t=timestamp,v1=hex(hmac_sha256(timestamp + "." + payload, secret))
		signedPayload := fmt.Sprintf("%d.%s", ts, payload)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signedPayload))
		signature := hex.EncodeToString(mac.Sum(nil))
		return "Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", ts, signature), nil

	case ProviderGitHub:
		// GitHub format: sha256=hex(hmac_sha256(payload, secret))
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(payload))
		signature := hex.EncodeToString(mac.Sum(nil))
		return "X-Hub-Signature-256", fmt.Sprintf("sha256=%s", signature), nil

	case ProviderRazorpay:
		// Razorpay format: hex(hmac_sha256(payload, secret))
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(payload))
		signature := hex.EncodeToString(mac.Sum(nil))
		return "X-Razorpay-Signature", signature, nil

	case ProviderShopify:
		// Shopify format: base64(hmac_sha256(payload, secret))
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(payload))
		signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		return "X-Shopify-Hmac-Sha256", signature, nil

	case ProviderSlack:
		// Slack format: v0=hex(hmac_sha256("v0:" + timestamp + ":" + payload, secret))
		signedPayload := fmt.Sprintf("v0:%d:%s", ts, payload)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signedPayload))
		signature := hex.EncodeToString(mac.Sum(nil))
		return "X-Slack-Signature", fmt.Sprintf("v0=%s", signature), nil

	case ProviderGeneric:
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(payload))
		signature := hex.EncodeToString(mac.Sum(nil))
		return "X-Signature-256", signature, nil

	default:
		return "", "", fmt.Errorf("unsupported webhook provider: %s", provider)
	}
}

// ResignHeaders updates request headers with newly calculated webhook signatures.
func ResignHeaders(headers map[string]string, provider string, secret string, payload string) (map[string]string, error) {
	out := make(map[string]string, len(headers)+2)
	for k, v := range headers {
		out[k] = v
	}

	key, val, err := CalculateSignature(provider, secret, payload, 0)
	if err != nil {
		return headers, err
	}

	out[key] = val

	if strings.ToLower(provider) == ProviderSlack {
		out["X-Slack-Request-Timestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
	}

	// Add audit tag
	out["X-DevHub-Resigned-Webhook"] = provider
	return out, nil
}

// BypassHeaders strips existing signature headers and injects testing bypass indicators.
func BypassHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers)+1)
	signatureKeys := []string{
		"stripe-signature",
		"x-hub-signature-256",
		"x-hub-signature",
		"x-razorpay-signature",
		"x-shopify-hmac-sha256",
		"x-slack-signature",
	}

	for k, v := range headers {
		lower := strings.ToLower(k)
		isSig := false
		for _, sigKey := range signatureKeys {
			if lower == sigKey {
				isSig = true
				break
			}
		}
		if !isSig {
			out[k] = v
		}
	}

	out["X-DevHub-Test-Mode"] = "true"
	out["X-DevHub-Signature-Bypass"] = "true"
	return out
}

// DetectProvider inspects incoming headers to identify if the request is a known webhook.
func DetectProvider(headers map[string]string) string {
	for k := range headers {
		lower := strings.ToLower(k)
		switch lower {
		case "stripe-signature":
			return ProviderStripe
		case "x-hub-signature-256", "x-hub-signature":
			return ProviderGitHub
		case "x-razorpay-signature":
			return ProviderRazorpay
		case "x-shopify-hmac-sha256":
			return ProviderShopify
		case "x-slack-signature":
			return ProviderSlack
		}
	}
	return ""
}
