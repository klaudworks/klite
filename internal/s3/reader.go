package s3

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

// Reader provides S3 range reads with footer caching for efficient fetch.
type Reader struct {
	client *Client
	logger *slog.Logger

	// Footer cache: S3 key -> parsed footer
	footerMu    sync.RWMutex
	footerCache map[string]*Footer

	// Object listing cache: topic/partition prefix -> sorted list of object keys+sizes
	listingMu    sync.RWMutex
	listingCache map[string][]ObjectInfo

	// Track range requests for testing
	mu            sync.Mutex
	rangeRequests []RangeRequestInfo
}

// RangeRequestInfo records range request details for test assertions.
type RangeRequestInfo struct {
	Key       string
	StartByte int64
	EndByte   int64
}

// NewReader creates a new S3 reader with caching.
func NewReader(client *Client, logger *slog.Logger) *Reader {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reader{
		client:       client,
		logger:       logger,
		footerCache:  make(map[string]*Footer),
		listingCache: make(map[string][]ObjectInfo),
	}
}

// Fetch reads batches from S3 starting at the given offset, up to maxBytes.
// Returns concatenated RecordBatch bytes.
func (r *Reader) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, partition)

	// 1. Find the right S3 object
	key, objectSize, err := r.findObjectForOffset(ctx, prefix, offset)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, nil // no data in S3 for this offset
	}

	// 2. Read the footer (cached after first access)
	footer, err := r.GetFooter(ctx, key, objectSize)
	if err != nil {
		return nil, fmt.Errorf("read footer for %s: %w", key, err)
	}

	if len(footer.Entries) == 0 {
		return nil, nil
	}

	// 3. Binary search for the first batch containing offset
	startIdx := footer.FindBatch(offset)
	if startIdx >= len(footer.Entries) {
		return nil, nil
	}

	// 4. Compute byte range: from the target batch to maxBytes
	startByte := int64(footer.Entries[startIdx].BytePosition)
	endByte := r.computeEndByte(footer, startIdx, maxBytes)

	// 5. Range GET only the needed bytes
	data, err := r.client.RangeGet(ctx, key, startByte, endByte)
	if err != nil {
		return nil, fmt.Errorf("range get %s: %w", key, err)
	}

	// Record range request for testing
	r.mu.Lock()
	r.rangeRequests = append(r.rangeRequests, RangeRequestInfo{
		Key:       key,
		StartByte: startByte,
		EndByte:   endByte,
	})
	r.mu.Unlock()

	return data, nil
}

// FetchBatches reads batches from S3 and returns them as individual BatchData slices.
// More useful when the caller needs per-batch metadata.
func (r *Reader) FetchBatches(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]BatchData, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, partition)

	key, objectSize, err := r.findObjectForOffset(ctx, prefix, offset)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, nil
	}

	footer, err := r.GetFooter(ctx, key, objectSize)
	if err != nil {
		return nil, fmt.Errorf("read footer for %s: %w", key, err)
	}

	if len(footer.Entries) == 0 {
		return nil, nil
	}

	startIdx := footer.FindBatch(offset)
	if startIdx >= len(footer.Entries) {
		return nil, nil
	}

	// Determine which batches to fetch
	var batchEntries []BatchIndexEntry
	var totalBytes int32
	for i := startIdx; i < len(footer.Entries); i++ {
		e := footer.Entries[i]
		batchSize := int32(e.BatchLength)
		// KIP-74: always include at least one batch
		if len(batchEntries) > 0 && totalBytes+batchSize > maxBytes {
			break
		}
		batchEntries = append(batchEntries, e)
		totalBytes += batchSize
	}

	if len(batchEntries) == 0 {
		return nil, nil
	}

	// Range GET
	startByte := int64(batchEntries[0].BytePosition)
	lastEntry := batchEntries[len(batchEntries)-1]
	endByte := int64(lastEntry.BytePosition) + int64(lastEntry.BatchLength)

	data, err := r.client.RangeGet(ctx, key, startByte, endByte)
	if err != nil {
		return nil, fmt.Errorf("range get %s: %w", key, err)
	}

	// Record range request
	r.mu.Lock()
	r.rangeRequests = append(r.rangeRequests, RangeRequestInfo{
		Key:       key,
		StartByte: startByte,
		EndByte:   endByte,
	})
	r.mu.Unlock()

	// Split data into individual batches
	var result []BatchData
	for _, e := range batchEntries {
		relativeStart := int64(e.BytePosition) - startByte
		relativeEnd := relativeStart + int64(e.BatchLength)
		if relativeEnd > int64(len(data)) {
			break
		}
		result = append(result, BatchData{
			RawBytes:        data[relativeStart:relativeEnd],
			BaseOffset:      e.BaseOffset,
			LastOffsetDelta: e.LastOffsetDelta,
		})
	}

	return result, nil
}

// RangeRequests returns recorded range requests (for test assertions).
func (r *Reader) RangeRequests() []RangeRequestInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]RangeRequestInfo, len(r.rangeRequests))
	copy(result, r.rangeRequests)
	return result
}

// findObjectForOffset finds the S3 object that contains the given offset.
// Returns (key, objectSize) or ("", 0) if no object contains the offset.
func (r *Reader) findObjectForOffset(ctx context.Context, prefix string, offset int64) (string, int64, error) {
	objects, err := r.getObjectListing(ctx, prefix)
	if err != nil {
		return "", 0, err
	}
	if len(objects) == 0 {
		return "", 0, nil
	}

	// Objects are sorted by key (which is zero-padded offset).
	// Find the last object whose base offset <= the requested offset.
	targetKey := prefix + ZeroPadOffset(offset) + ".obj"

	idx := sort.Search(len(objects), func(i int) bool {
		return objects[i].Key > targetKey
	})
	idx-- // step back to last key <= target

	if idx < 0 {
		// Offset is before all objects — try the first object
		// (it may contain the offset if there's a gap)
		return objects[0].Key, objects[0].Size, nil
	}

	return objects[idx].Key, objects[idx].Size, nil
}

// getObjectListing returns the cached object listing for a prefix.
func (r *Reader) getObjectListing(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	r.listingMu.RLock()
	listing, ok := r.listingCache[prefix]
	r.listingMu.RUnlock()
	if ok {
		return listing, nil
	}

	objects, err := r.client.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}

	r.listingMu.Lock()
	r.listingCache[prefix] = objects
	r.listingMu.Unlock()

	return objects, nil
}

// GetFooter returns the cached footer for an S3 object key.
func (r *Reader) GetFooter(ctx context.Context, key string, objectSize int64) (*Footer, error) {
	r.footerMu.RLock()
	footer, ok := r.footerCache[key]
	r.footerMu.RUnlock()
	if ok {
		return footer, nil
	}

	// Speculative read: last 64 KiB
	readSize := int64(DefaultFooterReadSize)
	if readSize > objectSize {
		readSize = objectSize
	}

	tailData, totalSize, err := r.client.TailGet(ctx, key, readSize)
	if err != nil {
		return nil, err
	}
	if totalSize == 0 {
		totalSize = objectSize
	}

	footer, err = ParseFooter(tailData, totalSize)
	if err != nil {
		// Check if we need a second read (footer larger than 64 KiB)
		if strings.Contains(err.Error(), "need second read") {
			// Read the full footer
			tailData, totalSize, err = r.client.TailGet(ctx, key, objectSize)
			if err != nil {
				return nil, err
			}
			footer, err = ParseFooter(tailData, totalSize)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	r.footerMu.Lock()
	r.footerCache[key] = footer
	r.footerMu.Unlock()

	return footer, nil
}

// computeEndByte computes the end byte for a range GET starting at startIdx,
// respecting the maxBytes limit.
func (r *Reader) computeEndByte(footer *Footer, startIdx int, maxBytes int32) int64 {
	var totalBytes int32
	var endByte int64

	for i := startIdx; i < len(footer.Entries); i++ {
		e := footer.Entries[i]
		batchSize := int32(e.BatchLength)
		// KIP-74: always include at least one batch
		if i > startIdx && totalBytes+batchSize > maxBytes {
			break
		}
		totalBytes += batchSize
		endByte = int64(e.BytePosition) + int64(e.BatchLength)
	}

	return endByte
}

// InvalidateFooters evicts all cached footers for a topic/partition.
// Called when compaction rewrites objects.
func (r *Reader) InvalidateFooters(topic string, partition int32) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, partition)

	r.footerMu.Lock()
	for key := range r.footerCache {
		if strings.HasPrefix(key, prefix) {
			delete(r.footerCache, key)
		}
	}
	r.footerMu.Unlock()

	r.listingMu.Lock()
	delete(r.listingCache, prefix)
	r.listingMu.Unlock()
}

// InvalidateAll clears all caches. Called during disaster recovery.
func (r *Reader) InvalidateAll() {
	r.footerMu.Lock()
	r.footerCache = make(map[string]*Footer)
	r.footerMu.Unlock()

	r.listingMu.Lock()
	r.listingCache = make(map[string][]ObjectInfo)
	r.listingMu.Unlock()
}

// FooterCacheSize returns the number of cached footers (for test assertions).
func (r *Reader) FooterCacheSize() int {
	r.footerMu.RLock()
	defer r.footerMu.RUnlock()
	return len(r.footerCache)
}

// DiscoverHW discovers the high-water mark for a topic/partition by inspecting
// the last S3 object's footer. Returns 0 if no objects exist.
func (r *Reader) DiscoverHW(ctx context.Context, topic string, partition int32) (int64, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, partition)

	objects, err := r.getObjectListing(ctx, prefix)
	if err != nil {
		return 0, err
	}
	if len(objects) == 0 {
		return 0, nil
	}

	// Last object contains the highest offsets
	lastObj := objects[len(objects)-1]
	footer, err := r.GetFooter(ctx, lastObj.Key, lastObj.Size)
	if err != nil {
		return 0, fmt.Errorf("read footer for %s: %w", lastObj.Key, err)
	}

	lastOff := footer.LastOffset()
	if lastOff < 0 {
		return 0, nil
	}
	return lastOff + 1, nil // HW = lastOffset + 1
}
