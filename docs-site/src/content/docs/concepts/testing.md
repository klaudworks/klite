---
title: Testing
description: How klite is tested and what that means for your data.
---

klite is tested at three layers: unit tests for internal logic, integration tests that exercise the real Kafka wire protocol, and benchmarks that catch performance regressions and stability issues under load.

## Unit tests

At the time of writing, 182 unit tests cover handler logic, wire encoding, offset assignment, WAL operations, state machine transitions, and error paths.

## Integration tests

At the time of writing, 159 integration tests each start a real klite broker in-process on a random port, connect a real [franz-go](https://github.com/twmb/franz-go) client over TCP, and exercise the Kafka wire protocol end to end. No mocks. No stubs.

Test scenarios are derived from two sources:

- **kfake's test suite** -- franz-go's in-memory Kafka implementation includes tests translated directly from Apache Kafka's own Java integration tests. klite's tests cover the same scenarios.
- **Kafka's test suite** -- edge cases and error paths are sourced from Kafka's own unit and integration tests.

S3 integration tests use [LocalStack](https://localstack.cloud/) via testcontainers, exercising the real S3 API (PutObject, GetObject, ListObjects) without requiring AWS credentials or a network connection. Crash recovery tests produce records, kill the broker, restart it, and confirm every acknowledged record is still readable.

## Race and leak detection

All tests run with Go's [race detector](https://go.dev/doc/articles/race_detector) enabled. The race detector instruments every memory access and catches data races, unsafe shared state, and missing synchronization that code review alone cannot reliably find.

On top of that, every test package uses [goleak](https://github.com/uber-go/goleak) to verify that no goroutines are leaked after tests complete. klite spawns goroutines per connection, per consumer group, for the WAL writer, for retention and compaction loops, and for the S3 flush pipeline. A leaked goroutine means a resource leak. goleak catches them.

## Benchmarks

The [throughput benchmark](/performance/stress-test/) pushes 1 billion records through the broker at maximum speed -- 553K events/sec sustained for 31 minutes. The [latency benchmark](/performance/benchmarks/) sustains 100K events/sec for one hour (360 million records) and measures end-to-end latency including network round-trips.

These are behavior-under-load tests, not just performance measurements. They run on real AWS infrastructure and verify two invariants after every run: every produced record was flushed to S3, and every produced record was consumed back successfully. If latency degrades, memory leaks, records go missing, or the broker crashes under sustained load, the benchmark catches it.
