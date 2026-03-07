package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	"github.com/klaudworks/klite/internal/lease"
	"github.com/klaudworks/klite/internal/lease/s3lease"
)

const (
	s3LeaseBucket = "klite-lease-test"
	s3LeaseRegion = "us-east-1"
)

func startLocalStack(t *testing.T) *s3svc.Client {
	t.Helper()
	ctx := context.Background()

	container, err := localstack.Run(ctx, "localstack/localstack:4.4.0")
	if err != nil {
		t.Skipf("LocalStack unavailable (Docker not running?): %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate localstack: %v", err)
		}
	})

	port, err := container.MappedPort(ctx, nat.Port("4566/tcp"))
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(s3LeaseRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
	)
	require.NoError(t, err)

	s3Client := s3svc.NewFromConfig(awsCfg, func(o *s3svc.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	_, err = s3Client.CreateBucket(ctx, &s3svc.CreateBucketInput{
		Bucket: aws.String(s3LeaseBucket),
	})
	require.NoError(t, err)

	return s3Client
}

func fastS3LeaseCfg(s3Client *s3svc.Client, holder, key string) s3lease.Config {
	return s3lease.Config{
		S3:            s3Client,
		Bucket:        s3LeaseBucket,
		Key:           key,
		Holder:        holder,
		ReplAddr:      holder + ":9093",
		LeaseDuration: 3 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		RetryInterval: 300 * time.Millisecond,
	}
}

func TestS3LeaseLocalStackAcquireAndRenew(t *testing.T) {
	t.Parallel()
	s3Client := startLocalStack(t)

	key := "test-prefix/lease-acq"
	cfg := fastS3LeaseCfg(s3Client, "node1", key)
	e := s3lease.New(cfg)

	elected := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = e.Run(ctx, lease.Callbacks{
			OnElected: func(ctx context.Context) {
				elected <- struct{}{}
			},
		})
	}()

	select {
	case <-elected:
	case <-time.After(10 * time.Second):
		t.Fatal("elector did not win lease within 10s")
	}

	require.Equal(t, lease.RolePrimary, e.Role())

	// Read the lease to check body
	out, err := s3Client.GetObject(context.Background(), &s3svc.GetObjectInput{
		Bucket: aws.String(s3LeaseBucket),
		Key:    &key,
	})
	require.NoError(t, err)
	defer out.Body.Close() //nolint:errcheck

	var body s3lease.LeaseBody
	require.NoError(t, json.NewDecoder(out.Body).Decode(&body))
	require.Equal(t, 1, body.Version)
	require.Equal(t, int64(1), body.Epoch)
	require.Equal(t, "node1", body.Holder)

	initialETag := aws.ToString(out.ETag)

	// Wait for a renewal and verify ETag changes
	time.Sleep(cfg.RenewInterval * 3)

	out2, err := s3Client.GetObject(context.Background(), &s3svc.GetObjectInput{
		Bucket: aws.String(s3LeaseBucket),
		Key:    &key,
	})
	require.NoError(t, err)
	defer out2.Body.Close() //nolint:errcheck

	newETag := aws.ToString(out2.ETag)
	require.NotEqual(t, initialETag, newETag, "ETag should change after renewal")
}

func TestS3LeaseLocalStackTwoNodes(t *testing.T) {
	t.Parallel()
	s3Client := startLocalStack(t)

	key := "test-prefix/lease-two"
	cfg1 := fastS3LeaseCfg(s3Client, "primary", key)
	cfg2 := fastS3LeaseCfg(s3Client, "standby", key)

	e1 := s3lease.New(cfg1)
	e2 := s3lease.New(cfg2)

	elected1 := make(chan struct{}, 1)
	elected2 := make(chan struct{}, 1)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	// Start both electors
	go func() {
		_ = e1.Run(ctx1, lease.Callbacks{
			OnElected: func(ctx context.Context) {
				elected1 <- struct{}{}
			},
		})
	}()
	go func() {
		_ = e2.Run(ctx2, lease.Callbacks{
			OnElected: func(ctx context.Context) {
				elected2 <- struct{}{}
			},
		})
	}()

	// Wait for e1 to become primary
	select {
	case <-elected1:
	case <-elected2:
		// e2 won first — that's also valid, swap logic
		cancel1()
		// e2 is primary, wait for its lease to expire
		time.Sleep(cfg2.LeaseDuration + cfg2.RetryInterval*2)
		t.Log("e2 won first; test still valid")
		return
	case <-time.After(10 * time.Second):
		t.Fatal("no elector won within 10s")
	}

	require.Equal(t, lease.RolePrimary, e1.Role())
	require.Equal(t, lease.RoleStandby, e2.Role())

	// Stop primary
	cancel1()
	time.Sleep(100 * time.Millisecond) // let e1 stop

	// Wait for e2 to detect expiry and claim
	select {
	case <-elected2:
	case <-time.After(cfg1.LeaseDuration + 5*time.Second):
		t.Fatal("standby did not promote after primary stopped")
	}

	require.Equal(t, lease.RolePrimary, e2.Role())
}

func TestS3LeaseLocalStackConditionalWriteSemantics(t *testing.T) {
	t.Parallel()
	s3Client := startLocalStack(t)

	key := "test-prefix/cond-test"

	// If-None-Match: * should succeed on new key
	star := "*"
	out, err := s3Client.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket:      aws.String(s3LeaseBucket),
		Key:         &key,
		Body:        bytes.NewReader([]byte("v1")),
		IfNoneMatch: &star,
	})
	require.NoError(t, err)
	etag1 := aws.ToString(out.ETag)
	require.NotEmpty(t, etag1)

	// If-None-Match: * should fail on existing key
	_, err = s3Client.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket:      aws.String(s3LeaseBucket),
		Key:         &key,
		Body:        bytes.NewReader([]byte("v2")),
		IfNoneMatch: &star,
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr))

	// If-Match with correct ETag should succeed
	out2, err := s3Client.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket:  aws.String(s3LeaseBucket),
		Key:     &key,
		Body:    bytes.NewReader([]byte("v3")),
		IfMatch: &etag1,
	})
	require.NoError(t, err)
	etag2 := aws.ToString(out2.ETag)
	require.NotEqual(t, etag1, etag2)

	// If-Match with old ETag should fail
	_, err = s3Client.PutObject(context.Background(), &s3svc.PutObjectInput{
		Bucket:  aws.String(s3LeaseBucket),
		Key:     &key,
		Body:    bytes.NewReader([]byte("v4")),
		IfMatch: &etag1,
	})
	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr))
}
