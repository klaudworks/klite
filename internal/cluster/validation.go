package cluster

import (
	"unicode/utf8"
)

// ValidateTopicName checks if a topic name is valid per Kafka conventions.
// Returns an error string if invalid, or empty string if valid.
func ValidateTopicName(name string) string {
	if name == "" {
		return "topic name must not be empty"
	}
	if name == "." || name == ".." {
		return "topic name must not be '.' or '..'"
	}
	if len(name) > 249 {
		return "topic name must not exceed 249 characters"
	}
	if !utf8.ValidString(name) {
		return "topic name must be valid UTF-8"
	}
	for _, c := range name {
		if !isValidTopicChar(c) {
			return "topic name contains invalid character: " + string(c)
		}
	}
	return ""
}

// isValidTopicChar returns true if c is valid in a Kafka topic name.
// Valid chars: [a-zA-Z0-9._-]
func isValidTopicChar(c rune) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '.' || c == '_' || c == '-'
}
