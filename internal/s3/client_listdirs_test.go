package s3

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestClientListChildDirsPagesCommonPrefixes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	api := &forcedPageS3{InMemoryS3: NewInMemoryS3(), maxKeys: 1}
	client := NewClient(ClientConfig{
		S3Client: api,
		Bucket:   "test-bucket",
		Prefix:   "klite/test",
	})

	keys := []string{
		"klite/test/metadata.log",
		"klite/test/topic-a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0/00000000000000000000.obj",
		"klite/test/topic-a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/1/00000000000000000000.obj",
		"klite/test/topic-b-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/0/00000000000000000000.obj",
	}
	for _, key := range keys {
		require.NoError(t, client.PutObject(ctx, key, []byte("x")))
	}

	dirs, err := client.ListChildDirs(ctx, "klite/test/")
	require.NoError(t, err)
	require.Equal(t,
		[]string{
			"topic-a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"topic-b-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		dirs,
	)
	require.True(t, api.sawDelimiter)
}

type forcedPageS3 struct {
	*InMemoryS3
	maxKeys      int32
	sawDelimiter bool
}

func (p *forcedPageS3) ListObjectsV2(ctx context.Context, input *s3svc.ListObjectsV2Input, opts ...func(*s3svc.Options)) (*s3svc.ListObjectsV2Output, error) {
	copyInput := *input
	copyInput.MaxKeys = aws.Int32(p.maxKeys)
	if aws.ToString(copyInput.Delimiter) == "/" {
		p.sawDelimiter = true
	}
	return p.InMemoryS3.ListObjectsV2(ctx, &copyInput, opts...)
}
