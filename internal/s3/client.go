package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

type Client struct {
	s3     S3API
	bucket string
	prefix string
	logger *slog.Logger
}

type S3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type ClientConfig struct {
	S3Client S3API
	Bucket   string
	Prefix   string // Resolved prefix, e.g. "klite-<clusterID>" or "myprefix/klite-<clusterID>"
	Logger   *slog.Logger
}

func NewClient(cfg ClientConfig) *Client {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{
		s3:     cfg.S3Client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		logger: cfg.Logger,
	}
}

func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

func (c *Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer out.Body.Close() //nolint:errcheck // best-effort close

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read body %s: %w", key, err)
	}
	return data, nil
}

// RangeGet downloads bytes in [startByte, endByte) from an S3 object.
func (c *Client) RangeGet(ctx context.Context, key string, startByte, endByte int64) ([]byte, error) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", startByte, endByte-1) // HTTP range is inclusive
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Range:  &rangeStr,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 range get %s [%d-%d]: %w", key, startByte, endByte, err)
	}
	defer out.Body.Close() //nolint:errcheck // best-effort close

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read range body %s: %w", key, err)
	}
	return data, nil
}

func (c *Client) TailGet(ctx context.Context, key string, n int64) ([]byte, int64, error) {
	rangeStr := fmt.Sprintf("bytes=-%d", n)
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Range:  &rangeStr,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("s3 tail get %s (last %d bytes): %w", key, n, err)
	}
	defer out.Body.Close() //nolint:errcheck // best-effort close

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("s3 read tail body %s: %w", key, err)
	}

	var objectSize int64
	if out.ContentRange != nil {
		parts := strings.Split(*out.ContentRange, "/") // "bytes start-end/total"
		if len(parts) == 2 {
			_, _ = fmt.Sscanf(parts[1], "%d", &objectSize)
		}
	}
	if objectSize == 0 && out.ContentLength != nil {
		objectSize = *out.ContentLength
	}

	return data, objectSize, nil
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	var continuationToken *string

	for {
		out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &c.bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 list objects prefix=%s: %w", prefix, err)
		}

		for _, obj := range out.Contents {
			info := ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			objects = append(objects, info)
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return objects, nil
}

func (c *Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}

// DeleteObjectsBatch deletes multiple objects (best-effort, errors are logged).
func (c *Client) DeleteObjectsBatch(ctx context.Context, keys []string) {
	for _, key := range keys {
		if err := c.DeleteObject(ctx, key); err != nil {
			c.logger.Warn("s3 delete failed", "key", key, "err", err)
		}
	}
}

func (c *Client) HeadObject(ctx context.Context, key string) (int64, error) {
	out, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return 0, fmt.Errorf("s3 head %s: %w", key, err)
	}
	return aws.ToInt64(out.ContentLength), nil
}

func (c *Client) Bucket() string {
	return c.bucket
}

func (c *Client) Prefix() string {
	return c.prefix
}

// InMemoryS3 implements S3API using an in-memory map (for tests).
type InMemoryS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte
	etags         map[string]string
	etagCounter   int
	timestamps    map[string]time.Time
	RangeRequests []RangeRequest
}

type RangeRequest struct {
	Key      string
	RangeStr string
}

func NewInMemoryS3() *InMemoryS3 {
	return &InMemoryS3{
		objects:    make(map[string][]byte),
		etags:      make(map[string]string),
		timestamps: make(map[string]time.Time),
	}
}

func (m *InMemoryS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(input.Key)

	if input.IfNoneMatch != nil && *input.IfNoneMatch == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, &smithy.GenericAPIError{
				Code:    "PreconditionFailed",
				Message: "At least one of the pre-conditions you specified did not hold",
			}
		}
	}

	if input.IfMatch != nil {
		storedETag, exists := m.etags[key]
		if !exists || storedETag != *input.IfMatch {
			return nil, &smithy.GenericAPIError{
				Code:    "PreconditionFailed",
				Message: "At least one of the pre-conditions you specified did not hold",
			}
		}
	}

	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	m.etagCounter++
	etag := fmt.Sprintf("\"etag-%d\"", m.etagCounter)

	m.objects[key] = data
	m.etags[key] = etag
	m.timestamps[key] = time.Now()
	return &s3.PutObjectOutput{ETag: &etag}, nil
}

func (m *InMemoryS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(input.Key)
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s", key)
	}

	if input.Range != nil {
		rangeStr := *input.Range
		m.RangeRequests = append(m.RangeRequests, RangeRequest{Key: key, RangeStr: rangeStr})

		var start, end int64
		if strings.HasPrefix(rangeStr, "bytes=-") {
			var n int64
			_, _ = fmt.Sscanf(rangeStr, "bytes=-%d", &n)
			start = int64(len(data)) - n
			if start < 0 {
				start = 0
			}
			end = int64(len(data)) - 1
		} else {
			_, _ = fmt.Sscanf(rangeStr, "bytes=%d-%d", &start, &end)
		}

		if start < 0 {
			start = 0
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		slice := data[start : end+1]
		totalSize := int64(len(data))
		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize)
		contentLen := int64(len(slice))
		rangeETag := m.etags[key]

		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(slice)),
			ContentRange:  &contentRange,
			ContentLength: &contentLen,
			ETag:          &rangeETag,
		}, nil
	}

	contentLen := int64(len(data))
	etag := m.etags[key]
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: &contentLen,
		ETag:          &etag,
	}, nil
}

func (m *InMemoryS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix := aws.ToString(input.Prefix)
	var contents []types.Object
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			k := key
			sz := int64(len(data))
			obj := types.Object{
				Key:  &k,
				Size: &sz,
			}
			if ts, ok := m.timestamps[key]; ok {
				t := ts
				obj.LastModified = &t
			}
			contents = append(contents, obj)
		}
	}

	for i := range contents {
		for j := i + 1; j < len(contents); j++ {
			if aws.ToString(contents[i].Key) > aws.ToString(contents[j].Key) {
				contents[i], contents[j] = contents[j], contents[i]
			}
		}
	}

	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, nil
}

func (m *InMemoryS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, aws.ToString(input.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func (m *InMemoryS3) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := aws.ToString(input.Key)
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s", key)
	}
	size := int64(len(data))
	etag := m.etags[key]
	return &s3.HeadObjectOutput{
		ContentLength: &size,
		ETag:          &etag,
	}, nil
}

func (m *InMemoryS3) ObjectCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

func (m *InMemoryS3) GetRaw(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	return data, ok
}

func (m *InMemoryS3) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	return keys
}
