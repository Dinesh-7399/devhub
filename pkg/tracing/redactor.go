package tracing

import (
	"regexp"
	"strings"
)

var (
	// Regex pattern targeting sensitive JSON key/value pairs
	jsonSecretRegex = regexp.MustCompile(`(?i)"(password|secret|token|apikey|api_key|access_token|refresh_token|credit_card|card_number|cvv|ssn)"\s*:\s*"([^"]+)"`)
	// Regex pattern targeting Bearer tokens in headers
	bearerTokenRegex = regexp.MustCompile(`(?i)(Bearer\s+)[A-Za-z0-9_\-\.]{8,}`)
)

// RedactHeaders scrubs sensitive authentication tokens and cookies.
func RedactHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return headers
	}

	scrubbed := make(map[string]string, len(headers))
	for k, v := range headers {
		lowerKey := strings.ToLower(k)
		switch {
		case lowerKey == "authorization" || lowerKey == "proxy-authorization":
			if bearerTokenRegex.MatchString(v) {
				scrubbed[k] = bearerTokenRegex.ReplaceAllString(v, "${1}***REDACTED***")
			} else {
				scrubbed[k] = "***REDACTED***"
			}
		case lowerKey == "cookie" || lowerKey == "set-cookie":
			scrubbed[k] = "***REDACTED_COOKIE***"
		case strings.Contains(lowerKey, "token") || strings.Contains(lowerKey, "secret") || strings.Contains(lowerKey, "key"):
			scrubbed[k] = "***REDACTED***"
		default:
			scrubbed[k] = v
		}
	}
	return scrubbed
}

// RedactBody scrubs sensitive passwords, tokens, and credit card numbers from JSON payloads.
func RedactBody(body string) string {
	if body == "" {
		return ""
	}

	if jsonSecretRegex.MatchString(body) {
		body = jsonSecretRegex.ReplaceAllString(body, `"$1":"***REDACTED***"`)
	}

	return body
}
