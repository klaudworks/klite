---
title: Storage
description: How klite organizes data in S3 and how reads and writes flow through the system.
---

klite uses S3 as its primary storage. Local disk holds a write-ahead log (WAL) that buffers data briefly before it's flushed to S3. The WAL ensures no data is lost if the process crashes between flushes. Once data lands in S3, the local WAL segments are cleaned up automatically.

## Bucket layout

```
s3://my-bucket/
  klite-<clusterID>/
    orders-a1b2c3d4e5f6.../        ← topic name + topic UUID
      0/                            ← partition
        00000000000000000000.obj
        00000000000000085204.obj    ← base offset, zero-padded
        ...
      1/
        00000000000000000000.obj
        ...
    clicks-f7e8d9c0b1a2.../
      0/
        ...
```

Each `.obj` file contains a batch of RecordBatches for a single partition. File names are the base offset of the first record in the object, zero-padded to 20 digits.

The `--s3-prefix` flag prepends an additional path component before `klite-<clusterID>` if you need to share a bucket across environments.

## Object format

Every `.obj` file has the same structure: a data section followed by a batch index footer.

```
┌──────────────────────────────────────────────┐
│  RecordBatch 0  (raw Kafka v2 bytes)         │
│  RecordBatch 1                               │
│  ...                                         │
│  RecordBatch N                               │
├──────────────────────────────────────────────┤
│  Index entry 0            (32 bytes each)    │
│  Index entry 1                               │
│  ...                                         │
│  Index entry N                               │
├──────────────────────────────────────────────┤
│  entry count              (4 bytes, uint32)  │
│  magic 0x4B4C4958 "KLIX"  (4 bytes, uint32)  │
└──────────────────────────────────────────────┘
```

Each 32-byte index entry describes one RecordBatch:

```
baseOffset       int64    byte position within the data section
bytePosition     uint32   where the batch starts in the file
batchLength      uint32   size of the batch in bytes
lastOffsetDelta  int32    offset range within this batch
maxTimestamp     int64    newest timestamp in the batch
recordCount      int32    number of records
```

The footer makes reads fast. To fetch a specific offset, klite reads the last 64 KB of the object (one S3 GET), binary-searches the index to find the right batch, then issues a byte-range GET for just that slice. Most reads hit S3 exactly twice.

## Write path

1. Produce requests land in a pre-allocated **chunk pool** (512 MB by default). klite manages this memory itself to avoid garbage collection overhead.
2. By default, when a partition's buffered data reaches **64 MB** or has been sitting for **60 seconds**, klite builds an S3 object from the in-memory chunks and uploads it with a single `PutObject`.
3. After upload, the chunks are returned to the pool for reuse.

If the chunk pool reaches 75% capacity, an emergency flush is triggered to free up space. If the pool is fully exhausted, producers block until uploads complete and chunks are returned.

Data also goes to the local WAL so it survives a crash before the next S3 flush. The WAL is purely a durability buffer and is cleaned up once its data is confirmed in S3.

## Read path

klite checks three places in order:

1. **Memory** (chunk pool) for data that hasn't been flushed yet.
2. **Local WAL** (OS page cache) for recently flushed data still on disk.
3. **S3** using the byte-range read described above.

Tail consumers reading recent data almost always hit memory or page cache. S3 reads only happen for consumers that are behind or catching up.

## Compaction

For topics with `cleanup.policy=compact`, klite compacts directly on S3. It downloads a window of objects, deduplicates records by key (keeping only the latest), re-encodes the surviving records into new objects, and deletes the originals. Tombstones are removed after `delete.retention.ms` (default 24 hours).

Compaction is rate-limited to 50 MB/s of S3 reads by default so it doesn't compete with consumer fetches.
