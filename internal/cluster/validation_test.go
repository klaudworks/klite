package cluster

import (
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr string // substring of expected error, "" means valid
	}{
		{name: "valid simple", topic: "my-topic", wantErr: ""},
		{name: "valid with dots", topic: "my.topic.name", wantErr: ""},
		{name: "valid with underscores", topic: "my_topic_name", wantErr: ""},
		{name: "valid with digits", topic: "topic123", wantErr: ""},
		{name: "valid mixed", topic: "My-Topic_1.0", wantErr: ""},
		{name: "valid single char", topic: "a", wantErr: ""},
		{name: "valid 249 chars", topic: strings.Repeat("a", 249), wantErr: ""},

		{name: "empty", topic: "", wantErr: "must not be empty"},
		{name: "dot", topic: ".", wantErr: "must not be '.' or '..'"},
		{name: "dotdot", topic: "..", wantErr: "must not be '.' or '..'"},
		{name: "too long", topic: strings.Repeat("a", 250), wantErr: "must not exceed 249"},
		{name: "invalid char space", topic: "my topic", wantErr: "invalid character"},
		{name: "invalid char slash", topic: "my/topic", wantErr: "invalid character"},
		{name: "invalid char colon", topic: "my:topic", wantErr: "invalid character"},
		{name: "invalid UTF-8", topic: string([]byte{0xff, 0xfe}), wantErr: "valid UTF-8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateTopicName(tt.topic)
			if tt.wantErr == "" {
				if got != "" {
					t.Errorf("ValidateTopicName(%q) = %q, want no error", tt.topic, got)
				}
			} else {
				if !strings.Contains(got, tt.wantErr) {
					t.Errorf("ValidateTopicName(%q) = %q, want substring %q", tt.topic, got, tt.wantErr)
				}
			}
		})
	}
}
