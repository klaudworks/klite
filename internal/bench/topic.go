package bench

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreateTopic creates a topic with the given partition count.
// Returns nil if the topic already exists.
func CreateTopic(ctx context.Context, brokers []string, topic string, partitions int32) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("connecting to broker: %w", err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	if partitions <= 0 {
		partitions = 1
	}
	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}
	for _, t := range resp {
		if t.Err != nil {
			if t.Err.Error() == "TOPIC_ALREADY_EXISTS" {
				return nil
			}
			return fmt.Errorf("creating topic %s: %w", t.Topic, t.Err)
		}
	}
	return nil
}

// DeleteTopic deletes a topic. Returns nil if the topic doesn't exist.
func DeleteTopic(ctx context.Context, brokers []string, topic string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("connecting to broker: %w", err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	resp, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("deleting topic: %w", err)
	}
	for _, t := range resp {
		if t.Err != nil {
			if t.Err.Error() == "UNKNOWN_TOPIC_OR_PARTITION" {
				return nil
			}
			return fmt.Errorf("deleting topic %s: %w", t.Topic, t.Err)
		}
	}
	return nil
}
