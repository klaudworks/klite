//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestS3ProduceAndFlush produces records, stops the broker via SIGINT
// (triggering graceful shutdown + S3 flush), and verifies objects appear in LocalStack.
func TestS3ProduceAndFlush(t *testing.T) {
	binary := buildBroker(t)
	dataDir := t.TempDir()
	prefix := "klite/e2e/" + t.Name()

	bp := startBroker(t, binary, dataDir, prefix)

	topic := "e2e-produce-flush"
	admin := newAdminClient(t, bp.addr)
	_, err := admin.CreateTopic(t.Context(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := newKgoClient(t, bp.addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < 10; i++ {
		produceSync(t, producer, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("e2e-flush-%d", i)),
		})
	}

	// Graceful shutdown triggers S3 flush
	bp.stopGraceful(t)

	// Verify S3 has objects for the topic
	keys := listS3Objects(t, prefix)
	found := false
	for _, key := range keys {
		if strings.Contains(key, topic+"/0/") && strings.HasSuffix(key, ".obj") {
			found = true
			break
		}
	}
	require.True(t, found, "expected S3 object for %s/0, got keys: %v", topic, keys)
}

// TestS3RestartAndConsume produces records, flushes to S3, deletes local data,
// restarts the broker, and verifies records are consumable from S3.
func TestS3RestartAndConsume(t *testing.T) {
	binary := buildBroker(t)
	dataDir := t.TempDir()
	prefix := "klite/e2e/" + t.Name()
	topic := "e2e-restart-consume"
	numRecords := 15

	// Phase 1: produce + graceful stop (flush to S3)
	bp := startBroker(t, binary, dataDir, prefix)

	admin := newAdminClient(t, bp.addr)
	_, err := admin.CreateTopic(t.Context(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := newKgoClient(t, bp.addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < numRecords; i++ {
		produceSync(t, producer, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("e2e-restart-%d", i)),
		})
	}

	bp.stopGraceful(t)

	// Delete local WAL to force reads from S3
	os.RemoveAll(dataDir + "/wal")

	// Phase 2: restart with same prefix — should find data in S3
	bp2 := startBroker(t, binary, dataDir, prefix)

	consumer := newKgoClient(t, bp2.addr,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	consumed := consumeN(t, consumer, numRecords, 30*time.Second)
	require.Len(t, consumed, numRecords)
	for i, r := range consumed {
		require.Equal(t, fmt.Sprintf("e2e-restart-%d", i), string(r.Value), "record %d", i)
	}
}

// TestS3GracefulShutdown verifies that SIGINT triggers a clean shutdown
// with S3 flush, and the process exits with code 0.
func TestS3GracefulShutdown(t *testing.T) {
	binary := buildBroker(t)
	dataDir := t.TempDir()
	prefix := "klite/e2e/" + t.Name()
	topic := "e2e-graceful-shutdown"

	bp := startBroker(t, binary, dataDir, prefix)

	admin := newAdminClient(t, bp.addr)
	_, err := admin.CreateTopic(t.Context(), 1, 1, nil, topic)
	require.NoError(t, err)

	producer := newKgoClient(t, bp.addr,
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	for i := 0; i < 5; i++ {
		produceSync(t, producer, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("e2e-shutdown-%d", i)),
		})
	}

	// SIGINT should trigger graceful shutdown + S3 flush + exit 0
	bp.stopGraceful(t)

	// Verify data made it to S3
	keys := listS3Objects(t, prefix)
	found := false
	for _, key := range keys {
		if strings.Contains(key, topic+"/0/") && strings.HasSuffix(key, ".obj") {
			found = true
			break
		}
	}
	require.True(t, found, "graceful shutdown should flush to S3, got keys: %v", keys)
}
