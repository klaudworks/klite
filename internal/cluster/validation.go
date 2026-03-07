package cluster

import (
	"unicode/utf8"
)

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

func isValidTopicChar(c rune) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '.' || c == '_' || c == '-'
}
