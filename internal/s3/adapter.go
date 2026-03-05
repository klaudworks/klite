package s3

import (
	"context"

	"github.com/klaudworks/klite/internal/cluster"
)

// ReaderAdapter adapts an s3.Reader to the cluster.S3Fetcher interface.
type ReaderAdapter struct {
	Reader *Reader
}

// FetchBatches implements cluster.S3Fetcher.
func (a *ReaderAdapter) FetchBatches(ctx context.Context, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) ([]cluster.S3BatchData, error) {
	batches, err := a.Reader.FetchBatches(ctx, topic, topicID, partition, offset, maxBytes)
	if err != nil {
		return nil, err
	}

	result := make([]cluster.S3BatchData, len(batches))
	for i, b := range batches {
		result[i] = cluster.S3BatchData{
			RawBytes:        b.RawBytes,
			BaseOffset:      b.BaseOffset,
			LastOffsetDelta: b.LastOffsetDelta,
		}
	}
	return result, nil
}
