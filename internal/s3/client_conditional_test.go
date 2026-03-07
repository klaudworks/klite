package s3

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestInMemoryS3ConditionalPutIfMatch(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	out, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:  aws.String("key1"),
		Body: bytes.NewReader([]byte("v1")),
	})
	require.NoError(t, err)
	etag1 := aws.ToString(out.ETag)
	require.NotEmpty(t, etag1)

	out2, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:     aws.String("key1"),
		Body:    bytes.NewReader([]byte("v2")),
		IfMatch: &etag1,
	})
	require.NoError(t, err)
	etag2 := aws.ToString(out2.ETag)
	require.NotEmpty(t, etag2)
	require.NotEqual(t, etag1, etag2)

	data, ok := m.GetRaw("key1")
	require.True(t, ok)
	require.Equal(t, []byte("v2"), data)
}

func TestInMemoryS3ConditionalPutIfMatchFail(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	_, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:  aws.String("key1"),
		Body: bytes.NewReader([]byte("v1")),
	})
	require.NoError(t, err)

	wrongETag := "\"wrong-etag\""
	_, err = m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:     aws.String("key1"),
		Body:    bytes.NewReader([]byte("v2")),
		IfMatch: &wrongETag,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "PreconditionFailed", apiErr.ErrorCode())

	data, ok := m.GetRaw("key1")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), data)
}

func TestInMemoryS3ConditionalPutIfMatchNoObject(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	etag := "\"some-etag\""
	_, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:     aws.String("key1"),
		Body:    bytes.NewReader([]byte("v1")),
		IfMatch: &etag,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
}

func TestInMemoryS3ConditionalPutIfNoneMatch(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	star := "*"
	out, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:         aws.String("key1"),
		Body:        bytes.NewReader([]byte("v1")),
		IfNoneMatch: &star,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.ETag))

	data, ok := m.GetRaw("key1")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), data)
}

func TestInMemoryS3ConditionalPutIfNoneMatchFail(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	_, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:  aws.String("key1"),
		Body: bytes.NewReader([]byte("v1")),
	})
	require.NoError(t, err)

	star := "*"
	_, err = m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:         aws.String("key1"),
		Body:        bytes.NewReader([]byte("v2")),
		IfNoneMatch: &star,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "PreconditionFailed", apiErr.ErrorCode())

	data, ok := m.GetRaw("key1")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), data)
}

func TestInMemoryS3ETagReturned(t *testing.T) {
	m := NewInMemoryS3()
	ctx := context.Background()

	putOut, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:  aws.String("key1"),
		Body: bytes.NewReader([]byte("data")),
	})
	require.NoError(t, err)
	putETag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, putETag)

	headOut, err := m.HeadObject(ctx, &s3svc.HeadObjectInput{
		Key: aws.String("key1"),
	})
	require.NoError(t, err)
	require.Equal(t, putETag, aws.ToString(headOut.ETag))

	getOut, err := m.GetObject(ctx, &s3svc.GetObjectInput{
		Key: aws.String("key1"),
	})
	require.NoError(t, err)
	require.Equal(t, putETag, aws.ToString(getOut.ETag))
	_ = getOut.Body.Close()

	putOut2, err := m.PutObject(ctx, &s3svc.PutObjectInput{
		Key:  aws.String("key1"),
		Body: bytes.NewReader([]byte("data2")),
	})
	require.NoError(t, err)
	putETag2 := aws.ToString(putOut2.ETag)
	require.NotEqual(t, putETag, putETag2)

	headOut2, err := m.HeadObject(ctx, &s3svc.HeadObjectInput{
		Key: aws.String("key1"),
	})
	require.NoError(t, err)
	require.Equal(t, putETag2, aws.ToString(headOut2.ETag))
}
