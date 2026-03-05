//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestGracefulShutdown verifies that SIGINT triggers a clean shutdown with S3
// flush and the process exits with code 0. This is the only behavior that
// requires a real subprocess — all data-flow logic (produce, flush, restart,
// consume) is covered by integration tests with InMemoryS3.
func TestGracefulShutdown(t *testing.T) {
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
		if strings.Contains(key, topic+"-") && strings.Contains(key, "/0/") && strings.HasSuffix(key, ".obj") {
			found = true
			break
		}
	}
	require.True(t, found, "graceful shutdown should flush to S3, got keys: %v", keys)
}

// TestS3APISmokeTest exercises each S3 API method once against real LocalStack.
// This verifies that the real AWS SDK client works with an actual HTTP endpoint,
// covering: PutObject, ListObjectsV2, GetObject (full + range), HeadObject, DeleteObject.
func TestS3APISmokeTest(t *testing.T) {
	ctx := context.Background()
	client, err := newS3Client(ctx)
	require.NoError(t, err)

	prefix := "klite/e2e/smoke"
	key := prefix + "/test-topic/0/00000000000000000000.obj"
	body := []byte("fake-s3-object-data-for-smoke-test")

	// --- PutObject ---
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "PutObject")

	// --- HeadObject ---
	headOut, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "HeadObject")
	require.Equal(t, int64(len(body)), aws.ToInt64(headOut.ContentLength), "HeadObject content length")

	// --- GetObject (full) ---
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "GetObject full")
	gotBody, err := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	require.NoError(t, err)
	require.Equal(t, body, gotBody, "GetObject body")

	// --- GetObject (range) ---
	rangeGet, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=5-14"),
	})
	require.NoError(t, err, "GetObject range")
	rangeBody, err := io.ReadAll(rangeGet.Body)
	rangeGet.Body.Close()
	require.NoError(t, err)
	require.Equal(t, body[5:15], rangeBody, "GetObject range body")

	// --- ListObjectsV2 ---
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(testBucket),
		Prefix: aws.String(prefix + "/"),
	})
	require.NoError(t, err, "ListObjectsV2")
	require.NotEmpty(t, listOut.Contents, "ListObjectsV2 should return objects")
	found := false
	for _, obj := range listOut.Contents {
		if aws.ToString(obj.Key) == key {
			found = true
			break
		}
	}
	require.True(t, found, "ListObjectsV2 should include the put key")

	// --- DeleteObject ---
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "DeleteObject")

	// Verify deletion
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	require.Error(t, err, "GetObject after delete should fail")
	var noSuchKey *s3types.NoSuchKey
	require.ErrorAs(t, err, &noSuchKey, "error should be NoSuchKey")
}
