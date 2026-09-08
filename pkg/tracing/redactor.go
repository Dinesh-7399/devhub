package tracing

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
)

var (
	// Regex pattern targeting Bearer tokens in headers
	bearerTokenRegex = regexp.MustCompile(`(?i)(Bearer\s+)[A-Za-z0-9_\-\.]{8,}`)
	// Fallback regex pattern for non-JSON unstructured data
	unstructuredSecretRegex = regexp.MustCompile(`(?i)"(password|secret|token|apikey|api_key|access_token|refresh_token|credit_card|card_number|cvv|ssn)"\s*:\s*"([^"]+)"`)
)

func isSensitiveKey(k string) bool {
	lower := strings.ToLower(k)
	return strings.Contains(lower, "password") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "apikey") ||
		strings.Contains(lower, "api_key") ||
		strings.Contains(lower, "cvv") ||
		strings.Contains(lower, "ssn") ||
		strings.Contains(lower, "credit_card") ||
		strings.Contains(lower, "card_number") ||
		strings.Contains(lower, "access_token") ||
		strings.Contains(lower, "refresh_token") ||
		strings.Contains(lower, "credential") ||
		strings.Contains(lower, "private_key")
}

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

// RedactBody scrubs sensitive passwords, tokens, and credit card numbers from JSON payloads, form-encoded data, and text.
func RedactBody(body string) string {
	if body == "" {
		return ""
	}

	trimmed := strings.TrimSpace(body)

	// 1. Try recursive JSON AST redaction (handles nested objects, arrays, and mixed types)
	if (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]")) {
		var parsed any
		if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
			redactedAST := redactAny(parsed)
			if outBytes, err := json.Marshal(redactedAST); err == nil {
				return string(outBytes)
			}
		}
	}

	// 2. Try Form-urlencoded parsing (key=val&secret=xxx)
	if strings.Contains(trimmed, "=") && !strings.Contains(trimmed, "\n") {
		if values, err := url.ParseQuery(trimmed); err == nil && len(values) > 0 {
			modified := false
			for k := range values {
				if isSensitiveKey(k) {
					values.Set(k, "***REDACTED***")
					modified = true
				}
			}
			if modified {
				return values.Encode()
			}
		}
	}

	// 3. Fallback regex scrubbing for unstructured text
	if unstructuredSecretRegex.MatchString(body) {
		body = unstructuredSecretRegex.ReplaceAllString(body, `"$1":"***REDACTED***"`)
	}

	return body
}

func redactAny(v any) any {
	switch val := v.(type) {
	case map[string]any:
		newMap := make(map[string]any, len(val))
		for k, item := range val {
			if isSensitiveKey(k) {
				newMap[k] = "***REDACTED***"
			} else {
				newMap[k] = redactAny(item)
			}
		}
		return newMap
	case []any:
		newList := make([]any, len(val))
		for i, item := range val {
			newList[i] = redactAny(item)
		}
		return newList
	default:
		return v
	}
}
