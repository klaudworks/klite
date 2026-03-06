---
title: Optimizations
description: Performance optimizations in klite and why they matter.
---

## Write path

<details>
<summary>Single interleaved WAL</summary>

Kafka uses one log file per partition. Most Kafka deployments skip fsync entirely and rely on replication for durability, but if you want single-node durability (which klite targets), that's one fsync per partition per write cycle. With 1000 partitions, that's 1000 fsyncs. On cloud volumes (EBS, Persistent Disk, etc.), each fsync is a network round-trip, so this adds up fast.

klite writes all partitions to a single write-ahead log. Entries from different partitions are interleaved in arrival order. One fsync makes everything in that file durable, regardless of how many partitions contributed.

</details>

<details>
<summary>Fsync batching</summary>

Even with a single WAL file, fsyncing after every write would be wasteful. klite collects writes over a short window (2ms by default), writes them all, then fsyncs once for the whole batch.

If 500 produce requests arrive within that 2ms window, they all share one fsync instead of competing for disk IOPS with hundreds of parallel fsyncs.

The window is tunable via `--wal-sync-interval`. Lower values reduce latency. Higher values let more writes accumulate per batch.

</details>

<details>
<summary>Async WAL segment pre-creation</summary>

Most cloud VMs use network-attached volumes (EBS on AWS, Persistent Disk on GCP, Managed Disks on Azure). Creating a new file and making it durable on these volumes requires a round-trip over the network, which can take 5-10ms under load.

klite writes to a WAL that rotates to a new file every 64 MB. Without pre-creation, every rotation would stall writes while the new file is created and the directory is synced.

To avoid this, klite creates the next segment file in the background while the current one is still being written to. By the time rotation happens, the new file is already open and ready. Rotation itself is just a pointer swap with zero IO on the write path.

</details>

## Memory

<details>
<summary>Pre-allocated chunk pool</summary>

klite allocates a fixed pool of memory buffers at startup (512 MB by default). Partitions borrow buffers from the pool to hold data that hasn't been flushed to S3 yet, and return them after upload.

This means the produce path never allocates memory for message storage during normal operation. There's no allocation churn and no garbage collection pressure from constantly creating and discarding buffers.

When the pool is 75% full, klite triggers an early S3 flush to free up space. If it's completely exhausted, producers wait until a buffer is returned. This keeps memory usage predictable -- you get backpressure instead of unbounded growth.

</details>

## Read path

<details>
<summary>S3 footer caching</summary>

Each S3 object has an index at the end (the "footer") that maps offsets to byte positions within the object. klite needs this index to figure out which bytes to request for a given fetch.

The first time klite reads from an object, it fetches the last 64 KB in a single request. That's enough to cover the index for most objects. The result is cached in memory, so subsequent fetches skip this step entirely.

After that, a consumer fetch is a single HTTP request for exactly the byte range it needs. No unnecessary data transfer, no repeated index lookups.

</details>
