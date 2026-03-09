package s3

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

type Reader struct {
	client *Client
	logger *slog.Logger

	footerMu    sync.RWMutex
	footerCache map[string]*Footer

	listingMu    sync.RWMutex
	listingCache map[string][]ObjectInfo

	mu            sync.Mutex
	rangeRequests []RangeRequestInfo
}

type RangeRequestInfo struct {
	Key       string
	StartByte int64
	EndByte   int64
}

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
func (r *Reader) Fetch(ctx context.Context, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)

	key, objectSize, nextKey, nextSize, err := r.findObjectForOffset(ctx, prefix, offset)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, nil // no data in S3 for this offset
	}

	data, err := r.fetchRawFromObject(ctx, key, objectSize, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return data, nil
	}

	if nextKey == "" {
		return nil, nil
	}
	return r.fetchRawFromObject(ctx, nextKey, nextSize, offset, maxBytes)
}

func (r *Reader) fetchRawFromObject(ctx context.Context, key string, objectSize, offset int64, maxBytes int32) ([]byte, error) {
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

	startByte := int64(footer.Entries[startIdx].BytePosition)
	endByte := r.computeEndByte(footer, startIdx, maxBytes)

	data, err := r.client.RangeGet(ctx, key, startByte, endByte)
	if err != nil {
		return nil, fmt.Errorf("range get %s: %w", key, err)
	}

	r.mu.Lock()
	r.rangeRequests = append(r.rangeRequests, RangeRequestInfo{
		Key:       key,
		StartByte: startByte,
		EndByte:   endByte,
	})
	r.mu.Unlock()

	return data, nil
}

func (r *Reader) FetchBatches(ctx context.Context, topic string, topicID [16]byte, partition int32, offset int64, maxBytes int32) ([]BatchData, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)

	key, objectSize, nextKey, nextSize, err := r.findObjectForOffset(ctx, prefix, offset)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, nil
	}

	result, err := r.fetchFromObject(ctx, key, objectSize, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	if len(result) > 0 {
		return result, nil
	}

	// Gap between objects — try the next one.
	if nextKey == "" {
		return nil, nil
	}
	return r.fetchFromObject(ctx, nextKey, nextSize, offset, maxBytes)
}

func (r *Reader) fetchFromObject(ctx context.Context, key string, objectSize, offset int64, maxBytes int32) ([]BatchData, error) {
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

	startByte := int64(batchEntries[0].BytePosition)
	lastEntry := batchEntries[len(batchEntries)-1]
	endByte := int64(lastEntry.BytePosition) + int64(lastEntry.BatchLength)

	data, err := r.client.RangeGet(ctx, key, startByte, endByte)
	if err != nil {
		return nil, fmt.Errorf("range get %s: %w", key, err)
	}

	r.mu.Lock()
	r.rangeRequests = append(r.rangeRequests, RangeRequestInfo{
		Key:       key,
		StartByte: startByte,
		EndByte:   endByte,
	})
	r.mu.Unlock()

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

func (r *Reader) RangeRequests() []RangeRequestInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]RangeRequestInfo, len(r.rangeRequests))
	copy(result, r.rangeRequests)
	return result
}

// findObjectForOffset returns the best matching object and the next object.
// The next object is returned so the caller can try it when the requested
// offset falls in a gap between objects.
func (r *Reader) findObjectForOffset(ctx context.Context, prefix string, offset int64) (key string, size int64, nextKey string, nextSize int64, err error) {
	objects, err := r.getObjectListing(ctx, prefix)
	if err != nil {
		return "", 0, "", 0, err
	}
	if len(objects) == 0 {
		return "", 0, "", 0, nil
	}

	targetKey := prefix + ZeroPadOffset(offset) + ".obj"

	idx := sort.Search(len(objects), func(i int) bool {
		return objects[i].Key > targetKey
	})
	idx-- // step back to last key <= target

	if idx < 0 {
		return objects[0].Key, objects[0].Size, "", 0, nil
	}

	key = objects[idx].Key
	size = objects[idx].Size
	if idx+1 < len(objects) {
		nextKey = objects[idx+1].Key
		nextSize = objects[idx+1].Size
	}
	return key, size, nextKey, nextSize, nil
}

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

func (r *Reader) GetFooter(ctx context.Context, key string, objectSize int64) (*Footer, error) {
	r.footerMu.RLock()
	footer, ok := r.footerCache[key]
	r.footerMu.RUnlock()
	if ok {
		return footer, nil
	}

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
		if strings.Contains(err.Error(), "need second read") {
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

func (r *Reader) InvalidateFooters(topic string, topicID [16]byte, partition int32) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)

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

func (r *Reader) InvalidateListings(topic string, topicID [16]byte, partition int32) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)
	r.listingMu.Lock()
	delete(r.listingCache, prefix)
	r.listingMu.Unlock()
}

// AppendToListing appends a newly flushed object to the cached listing for a
// partition. If the listing was invalidated (e.g. by compaction) or was never
// populated, this is a no-op — the next consumer fetch will re-LIST from S3.
//
// Flush only ever adds objects with strictly increasing offsets, so appending
// preserves the sort order that findObjectForOffset relies on.
func (r *Reader) AppendToListing(topic string, topicID [16]byte, partition int32, obj ObjectInfo) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)
	r.listingMu.Lock()
	defer r.listingMu.Unlock()
	existing, ok := r.listingCache[prefix]
	if !ok {
		return
	}
	r.listingCache[prefix] = append(existing, obj)
}

func (r *Reader) InvalidateAll() {
	r.footerMu.Lock()
	r.footerCache = make(map[string]*Footer)
	r.footerMu.Unlock()

	r.listingMu.Lock()
	r.listingCache = make(map[string][]ObjectInfo)
	r.listingMu.Unlock()
}

func (r *Reader) FooterCacheSize() int {
	r.footerMu.RLock()
	defer r.footerMu.RUnlock()
	return len(r.footerCache)
}

// DiscoverHW discovers the high-water mark by inspecting the last S3 object's
// footer. Returns 0 if no objects exist.
func (r *Reader) DiscoverHW(ctx context.Context, topic string, topicID [16]byte, partition int32) (int64, error) {
	prefix := ObjectKeyPrefix(r.client.prefix, topic, topicID, partition)

	objects, err := r.getObjectListing(ctx, prefix)
	if err != nil {
		return 0, err
	}
	if len(objects) == 0 {
		return 0, nil
	}

	lastObj := objects[len(objects)-1]
	footer, err := r.GetFooter(ctx, lastObj.Key, lastObj.Size)
	if err != nil {
		return 0, fmt.Errorf("read footer for %s: %w", lastObj.Key, err)
	}

	lastOff := footer.LastOffset()
	if lastOff < 0 {
		return 0, nil
	}
	return lastOff + 1, nil
}
