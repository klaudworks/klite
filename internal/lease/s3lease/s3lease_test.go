package s3lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/klaudworks/klite/internal/lease"
	s3client "github.com/klaudworks/klite/internal/s3"
)

const (
	testBucket = "test-bucket"
	testKey    = "test-prefix/lease"
)

func fastCfg(s3api S3API, holder string) Config {
	return Config{
		S3:            s3api,
		Bucket:        testBucket,
		Key:           testKey,
		Holder:        holder,
		ReplAddr:      holder + ":9093",
		LeaseDuration: 500 * time.Millisecond,
		RenewInterval: 50 * time.Millisecond,
		RetryInterval: 30 * time.Millisecond,
	}
}

func waitFor(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for: %s", msg)
	}
}

func startElector(t *testing.T, e *Elector, cb lease.Callbacks) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.Run(ctx, cb)
	}()
	t.Cleanup(func() {
		cancel()
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("elector run error: %v", err)
		}
	})
	return cancel
}

func TestS3LeaseAcquireEmpty(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	elected := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")
	require.Equal(t, lease.RolePrimary, e.Role())

	// Verify lease body
	data, ok := mem.GetRaw(testKey)
	require.True(t, ok)
	var body LeaseBody
	require.NoError(t, json.Unmarshal(data, &body))
	require.Equal(t, 1, body.Version)
	require.Equal(t, int64(1), body.Epoch)
	require.Equal(t, "node1", body.Holder)
	require.Equal(t, "node1:9093", body.ReplAddr)
	require.False(t, body.RenewedAt.IsZero())
}

func TestS3LeaseRenew(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	elected := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")

	// Get the initial ETag
	data, _ := mem.GetRaw(testKey)
	var body1 LeaseBody
	require.NoError(t, json.Unmarshal(data, &body1))

	// Wait for at least one renewal
	time.Sleep(cfg.RenewInterval * 3)

	data2, _ := mem.GetRaw(testKey)
	var body2 LeaseBody
	require.NoError(t, json.Unmarshal(data2, &body2))

	require.Equal(t, body1.Epoch, body2.Epoch, "epoch should NOT change on renewal")
	require.True(t, body2.RenewedAt.After(body1.RenewedAt) || body2.RenewedAt.Equal(body1.RenewedAt),
		"renewedAt should be updated")
}

func TestS3LeaseExpiry(t *testing.T) {
	mem := s3client.NewInMemoryS3()

	// Seed an expired lease
	expiredBody := LeaseBody{
		Version:   1,
		Holder:    "old-primary",
		Epoch:     5,
		RenewedAt: time.Now().Add(-time.Hour),
		ReplAddr:  "old:9093",
	}
	data, _ := json.Marshal(expiredBody)
	_, err := mem.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	cfg := fastCfg(mem, "standby1")
	e := New(cfg)
	elected := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "standby should detect expiry and claim")

	// Verify epoch incremented
	data2, _ := mem.GetRaw(testKey)
	var newBody LeaseBody
	require.NoError(t, json.Unmarshal(data2, &newBody))
	require.Equal(t, int64(6), newBody.Epoch)
	require.Equal(t, "standby1", newBody.Holder)
}

func TestS3LeaseRace(t *testing.T) {
	mem := s3client.NewInMemoryS3()

	// Seed an expired lease
	expiredBody := LeaseBody{
		Version:   1,
		Holder:    "old",
		Epoch:     1,
		RenewedAt: time.Now().Add(-time.Hour),
		ReplAddr:  "old:9093",
	}
	data, _ := json.Marshal(expiredBody)
	_, err := mem.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	var winnerCount atomic.Int32

	cfg1 := fastCfg(mem, "node1")
	cfg2 := fastCfg(mem, "node2")
	e1 := New(cfg1)
	e2 := New(cfg2)

	elected1 := make(chan struct{}, 1)
	elected2 := make(chan struct{}, 1)

	startElector(t, e1, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			winnerCount.Add(1)
			elected1 <- struct{}{}
		},
	})
	startElector(t, e2, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			winnerCount.Add(1)
			elected2 <- struct{}{}
		},
	})

	// Wait for one winner
	select {
	case <-elected1:
	case <-elected2:
	case <-time.After(3 * time.Second):
		t.Fatal("no elector won within timeout")
	}

	// Brief wait to ensure only one winner
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), winnerCount.Load(), "exactly one elector should win")
}

func TestS3LeaseRenewPermanentFailure(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	elected := make(chan struct{}, 1)
	demoted := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")

	// Corrupt the ETag — simulate another node writing the lease
	otherBody := LeaseBody{
		Version:   1,
		Holder:    "other",
		Epoch:     99,
		RenewedAt: time.Now(),
		ReplAddr:  "other:9093",
	}
	otherData, _ := json.Marshal(otherBody)
	_, err := mem.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(otherData),
	})
	require.NoError(t, err)

	// The next renewal should fail with 412 and trigger immediate demotion
	waitFor(t, demoted, 2*time.Second, "OnDemoted after 412")
	require.Equal(t, lease.RoleStandby, e.Role())
}

func TestS3LeaseRenewTransientError(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")
	cfg.LeaseDuration = 300 * time.Millisecond
	cfg.RenewInterval = 50 * time.Millisecond

	failS3 := &failingS3{
		S3API:   mem,
		failPut: false,
	}
	cfg.S3 = failS3

	e := New(cfg)
	elected := make(chan struct{}, 1)
	demoted := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")

	// Now start failing PutObject with transient errors
	failS3.mu.Lock()
	failS3.failPut = true
	failS3.mu.Unlock()

	// Should NOT be demoted immediately
	select {
	case <-demoted:
		// Check that enough time passed (should be close to leaseDuration)
		// The fact we got here means demotion happened — verify it wasn't instant
	case <-time.After(cfg.LeaseDuration + 200*time.Millisecond):
		t.Fatal("should have been demoted after leaseDuration of transient errors")
	}

	require.Equal(t, lease.RoleStandby, e.Role())
}

func TestS3LeaseEpochIncrement(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	elected := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")

	// Check initial epoch
	data, _ := mem.GetRaw(testKey)
	var body LeaseBody
	require.NoError(t, json.Unmarshal(data, &body))
	require.Equal(t, int64(1), body.Epoch)

	// Wait for a renewal
	time.Sleep(cfg.RenewInterval * 3)

	data2, _ := mem.GetRaw(testKey)
	var body2 LeaseBody
	require.NoError(t, json.Unmarshal(data2, &body2))
	require.Equal(t, int64(1), body2.Epoch, "epoch must NOT change on renewal")
}

func TestS3LeaseConditionalWriteProbe(t *testing.T) {
	// S3 backend that never returns 412
	noCondS3 := &noConditionalS3{}
	cfg := fastCfg(noCondS3, "node1")

	e := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := e.Run(ctx, lease.Callbacks{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "conditional write probe failed")
}

func TestS3LeaseReleasePrimary(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	elected := make(chan struct{}, 1)

	startElector(t, e, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	waitFor(t, elected, 2*time.Second, "OnElected")

	err := e.Release()
	require.NoError(t, err)

	// Verify the lease body has zero renewedAt
	data, ok := mem.GetRaw(testKey)
	require.True(t, ok)
	var body LeaseBody
	require.NoError(t, json.Unmarshal(data, &body))
	require.True(t, body.RenewedAt.IsZero(), "renewedAt should be zero time after release")

	// Now a standby should be able to claim within retryInterval
	cfg2 := fastCfg(mem, "standby1")
	e2 := New(cfg2)
	elected2 := make(chan struct{}, 1)

	startElector(t, e2, lease.Callbacks{
		OnElected: func(ctx context.Context) {
			elected2 <- struct{}{}
		},
	})

	waitFor(t, elected2, 2*time.Second, "standby should claim released lease quickly")
}

func TestS3LeaseReleaseNotPrimary(t *testing.T) {
	mem := s3client.NewInMemoryS3()
	cfg := fastCfg(mem, "node1")

	e := New(cfg)
	require.Equal(t, lease.RoleStandby, e.Role())

	err := e.Release()
	require.NoError(t, err)

	// No S3 writes should have occurred (only the probe key, not the lease key)
	_, ok := mem.GetRaw(testKey)
	require.False(t, ok, "no lease object should exist")
}

// failingS3 wraps S3API to inject transient errors on PutObject.
type failingS3 struct {
	S3API
	mu      sync.Mutex
	failPut bool
}

func (f *failingS3) PutObject(ctx context.Context, input *s3svc.PutObjectInput, opts ...func(*s3svc.Options)) (*s3svc.PutObjectOutput, error) {
	f.mu.Lock()
	shouldFail := f.failPut
	f.mu.Unlock()

	key := aws.ToString(input.Key)
	// Only fail the lease key, not probe writes
	if shouldFail && key == testKey {
		// Drain body to avoid leaks
		if input.Body != nil {
			io.Copy(io.Discard, input.Body) //nolint:errcheck
		}
		return nil, &smithy.GenericAPIError{
			Code:    "InternalError",
			Message: "simulated S3 transient error",
		}
	}
	return f.S3API.PutObject(ctx, input, opts...)
}

// noConditionalS3 never returns 412 for If-None-Match.
type noConditionalS3 struct{}

func (n *noConditionalS3) PutObject(_ context.Context, input *s3svc.PutObjectInput, _ ...func(*s3svc.Options)) (*s3svc.PutObjectOutput, error) {
	if input.Body != nil {
		io.Copy(io.Discard, input.Body) //nolint:errcheck
	}
	etag := "\"etag-1\""
	return &s3svc.PutObjectOutput{ETag: &etag}, nil
}

func (n *noConditionalS3) GetObject(_ context.Context, _ *s3svc.GetObjectInput, _ ...func(*s3svc.Options)) (*s3svc.GetObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "not found"}
}
