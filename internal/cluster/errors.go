package cluster

import "errors"

// ErrTopicNotFound is returned when a topic doesn't exist and auto-create is disabled.
var ErrTopicNotFound = errors.New("topic not found")
