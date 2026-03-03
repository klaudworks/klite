package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klaudworks/klite/internal/bench"
	s3fmt "github.com/klaudworks/klite/internal/s3"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "produce":
		if err := runProduce(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "consume":
		if err := runConsume(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "produce-consume":
		if err := runProduceConsume(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "create-topic":
		if err := runCreateTopic(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "delete-topic":
		if err := runDeleteTopic(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "s3-count":
		if err := runS3Count(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `klite-bench — Kafka-compatible performance testing tool

Usage:
  klite-bench produce [flags]           Run producer throughput/latency test
  klite-bench consume [flags]           Run consumer throughput test
  klite-bench produce-consume [flags]   Run both, measure e2e latency
  klite-bench create-topic [flags]      Create a topic
  klite-bench delete-topic [flags]      Delete a topic
  klite-bench s3-count [flags]          Count records in S3 objects

Run 'klite-bench <command> --help' for command-specific flags.
`)
}

func runProduce(args []string) error {
	cfg := bench.DefaultProducerConfig()

	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	var brokers string
	var jsonOut string
	fs.StringVar(&brokers, "bootstrap-server", "localhost:9092", "Broker address(es), comma-separated")
	fs.StringVar(&cfg.Topic, "topic", cfg.Topic, "Topic to produce to")
	fs.Int64Var(&cfg.NumRecords, "num-records", cfg.NumRecords, "Number of records to produce")
	fs.IntVar(&cfg.RecordSize, "record-size", cfg.RecordSize, "Record size in bytes")
	fs.Float64Var(&cfg.Throughput, "throughput", cfg.Throughput, "Throttle to approximately this many records/sec (-1 = unlimited)")
	fs.IntVar(&cfg.Acks, "acks", cfg.Acks, "Required acks: -1 (all), 0, 1")
	batchMax := fs.Int("batch-max-bytes", int(cfg.BatchMaxBytes), "Max bytes per batch")
	fs.IntVar(&cfg.LingerMs, "linger-ms", cfg.LingerMs, "Producer linger in milliseconds")
	fs.IntVar(&cfg.NumProducers, "producers", cfg.NumProducers, "Number of concurrent producer clients")
	fs.IntVar(&cfg.MaxBufferedRecords, "max-buffered-records", cfg.MaxBufferedRecords, "Max records buffered in client (controls backpressure)")
	fs.Int64Var(&cfg.WarmupRecords, "warmup-records", 0, "Number of initial records to discard from stats")
	reportingMs := fs.Int64("reporting-interval", 5000, "Reporting interval in milliseconds")
	fs.StringVar(&jsonOut, "json-output", "", "Path to write JSON results (optional)")

	fs.Parse(args)

	cfg.Brokers = splitBrokers(brokers)
	cfg.BatchMaxBytes = int32(*batchMax)
	cfg.ReportingInterval = time.Duration(*reportingMs) * time.Millisecond

	// Open JSON Lines output file if requested.
	if jsonOut != "" {
		f, err := os.Create(jsonOut)
		if err != nil {
			return fmt.Errorf("creating json output file: %w", err)
		}
		defer f.Close()
		cfg.JSONOut = f
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := bench.RunProducer(ctx, cfg)
	_ = result

	return err
}

func runConsume(args []string) error {
	cfg := bench.DefaultConsumerConfig()

	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	var brokers string
	var jsonOut string
	fs.StringVar(&brokers, "bootstrap-server", "localhost:9092", "Broker address(es), comma-separated")
	fs.StringVar(&cfg.Topic, "topic", cfg.Topic, "Topic to consume from")
	fs.Int64Var(&cfg.NumRecords, "num-records", cfg.NumRecords, "Number of records to consume")
	fetchMax := fs.Int("fetch-max-bytes", int(cfg.FetchMaxBytes), "Max fetch bytes per partition")
	fs.IntVar(&cfg.NumConsumers, "consumers", cfg.NumConsumers, "Number of concurrent consumers in the same group")
	timeoutMs := fs.Int64("timeout", 60000, "Max time in ms between received records before exiting")
	reportingMs := fs.Int64("reporting-interval", 5000, "Reporting interval in milliseconds")
	fs.BoolVar(&cfg.ShowDetailedStats, "show-detailed-stats", false, "Print per-interval stats")
	fs.StringVar(&jsonOut, "json-output", "", "Path to write JSON results (optional)")

	fs.Parse(args)

	cfg.Brokers = splitBrokers(brokers)
	cfg.FetchMaxBytes = int32(*fetchMax)
	cfg.Timeout = time.Duration(*timeoutMs) * time.Millisecond
	cfg.ReportingInterval = time.Duration(*reportingMs) * time.Millisecond

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := bench.RunConsumer(ctx, cfg)

	if jsonOut != "" && result != nil {
		writeConsumerJSON(jsonOut, cfg, result)
	}

	return err
}

func runProduceConsume(args []string) error {
	cfg := bench.DefaultProduceConsumeConfig()

	fs := flag.NewFlagSet("produce-consume", flag.ExitOnError)
	var brokers string
	var jsonOut string
	fs.StringVar(&brokers, "bootstrap-server", "localhost:9092", "Broker address(es), comma-separated")
	fs.StringVar(&cfg.Topic, "topic", cfg.Topic, "Topic to produce to and consume from")
	fs.Int64Var(&cfg.NumRecords, "num-records", cfg.NumRecords, "Number of records to produce")
	fs.IntVar(&cfg.RecordSize, "record-size", cfg.RecordSize, "Record value size in bytes")
	fs.Float64Var(&cfg.Throughput, "throughput", cfg.Throughput, "Records/sec cap (-1 = unlimited)")
	fs.IntVar(&cfg.Acks, "acks", cfg.Acks, "Required acks: -1 (all), 0, 1")
	batchMax := fs.Int("batch-max-bytes", int(cfg.BatchMaxBytes), "Max bytes per batch")
	fs.IntVar(&cfg.LingerMs, "linger-ms", cfg.LingerMs, "Producer linger in milliseconds")
	fs.IntVar(&cfg.NumProducers, "producers", cfg.NumProducers, "Number of concurrent producers")
	fs.IntVar(&cfg.NumConsumers, "consumers", cfg.NumConsumers, "Number of concurrent consumers")
	fs.IntVar(&cfg.MaxBufferedRecords, "max-buffered-records", cfg.MaxBufferedRecords, "Max records buffered in client")
	fs.Int64Var(&cfg.WarmupRecords, "warmup-records", 0, "Records to discard from stats")
	reportingMs := fs.Int64("reporting-interval", 5000, "Reporting interval in milliseconds")
	drainMs := fs.Int64("drain-timeout", 30000, "Max ms to wait for consumers after produce finishes")
	fs.StringVar(&jsonOut, "json-output", "", "Path to write JSON Lines results")

	fs.Parse(args)

	cfg.Brokers = splitBrokers(brokers)
	cfg.BatchMaxBytes = int32(*batchMax)
	cfg.ReportingInterval = time.Duration(*reportingMs) * time.Millisecond
	cfg.DrainTimeout = time.Duration(*drainMs) * time.Millisecond

	if jsonOut != "" {
		f, err := os.Create(jsonOut)
		if err != nil {
			return fmt.Errorf("creating json output file: %w", err)
		}
		defer f.Close()
		cfg.JSONOut = f
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, err := bench.RunProduceConsume(ctx, cfg)
	return err
}

func runCreateTopic(args []string) error {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	var brokers string
	fs.StringVar(&brokers, "bootstrap-server", "localhost:9092", "Broker address(es), comma-separated")
	topic := fs.String("topic", "bench", "Topic to create")
	partitions := fs.Int("partitions", 6, "Number of partitions")
	fs.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return bench.CreateTopic(ctx, splitBrokers(brokers), *topic, int32(*partitions))
}

func runDeleteTopic(args []string) error {
	fs := flag.NewFlagSet("delete-topic", flag.ExitOnError)
	var brokers string
	fs.StringVar(&brokers, "bootstrap-server", "localhost:9092", "Broker address(es), comma-separated")
	topic := fs.String("topic", "bench", "Topic to delete")
	fs.Parse(args)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return bench.DeleteTopic(ctx, splitBrokers(brokers), *topic)
}

func runS3Count(args []string) error {
	fs := flag.NewFlagSet("s3-count", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "S3 key prefix (e.g. <run_id>/)")
	region := fs.String("region", "eu-west-1", "AWS region")
	jsonOutput := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	if *bucket == "" {
		return fmt.Errorf("flag -bucket is required")
	}

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*region))
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	// List all objects under prefix.
	type objEntry struct {
		Key  string
		Size int64
	}
	var objects []objEntry
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: aws.String(*prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("listing objects: %w", err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, objEntry{*obj.Key, *obj.Size})
		}
	}

	var totalRecords int64
	var totalObjects int
	partitionRecords := map[string]int64{}

	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, "metadata.log") {
			continue
		}

		footer, err := fetchFooter(ctx, client, *bucket, obj.Key, obj.Size)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: %s: %v\n", obj.Key, err)
			continue
		}

		records := footer.TotalRecordCount()
		totalRecords += records
		totalObjects++

		parts := strings.Split(obj.Key, "/")
		if len(parts) >= 2 {
			partKey := parts[len(parts)-2]
			partitionRecords[partKey] += records
		}
	}

	if *jsonOutput {
		out := s3CountJSON{
			Bucket:     *bucket,
			Prefix:     *prefix,
			Objects:    totalObjects,
			Records:    totalRecords,
			Partitions: partitionRecords,
		}
		data, _ := json.Marshal(out)
		fmt.Println(string(data))
	} else {
		fmt.Printf("Bucket:  s3://%s/%s\n", *bucket, *prefix)
		fmt.Printf("Objects: %d\n", totalObjects)
		for p := 0; p < 100; p++ {
			pk := fmt.Sprintf("%d", p)
			if recs, ok := partitionRecords[pk]; ok {
				fmt.Printf("  Partition %s: %d records\n", pk, recs)
			}
		}
		fmt.Printf("Total:   %d records\n", totalRecords)
	}

	return nil
}

type s3CountJSON struct {
	Bucket     string           `json:"bucket"`
	Prefix     string           `json:"prefix"`
	Objects    int              `json:"objects"`
	Records    int64            `json:"records"`
	Partitions map[string]int64 `json:"partitions"`
}

// fetchFooter reads the footer of an S3 object. It first tries a 64 KiB tail
// read; if the footer is larger, it does a second read with the exact size.
func fetchFooter(ctx context.Context, client *s3.Client, bucket, key string, objectSize int64) (*s3fmt.Footer, error) {
	readSize := int64(64 * 1024)
	if readSize > objectSize {
		readSize = objectSize
	}

	tailData, err := s3RangeRead(ctx, client, bucket, key, objectSize, readSize)
	if err != nil {
		return nil, err
	}

	footer, err := s3fmt.ParseFooter(tailData, objectSize)
	if err != nil && strings.Contains(err.Error(), "need second read") {
		// The trailer tells us the entry count; compute exact footer size.
		trailerSize := int64(s3fmt.FooterTrailerSize)
		tailLen := int64(len(tailData))
		entryCount := int64(binary.BigEndian.Uint32(tailData[tailLen-8 : tailLen-4]))
		needed := entryCount*int64(s3fmt.IndexEntrySize) + trailerSize
		if needed > objectSize {
			needed = objectSize
		}

		tailData, err = s3RangeRead(ctx, client, bucket, key, objectSize, needed)
		if err != nil {
			return nil, err
		}
		footer, err = s3fmt.ParseFooter(tailData, objectSize)
	}
	return footer, err
}

func s3RangeRead(ctx context.Context, client *s3.Client, bucket, key string, objectSize, readSize int64) ([]byte, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", objectSize-readSize, objectSize-1)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject range read: %w", err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// JSON output helpers

type produceJSON struct {
	Type       string  `json:"type"`
	Brokers    string  `json:"brokers"`
	Topic      string  `json:"topic"`
	Producers  int     `json:"producers"`
	RecordSize int     `json:"record_size"`
	Acks       int     `json:"acks"`
	BatchMax           int     `json:"batch_max_bytes"`
	LingerMs           int     `json:"linger_ms"`
	MaxBufferedRecords int     `json:"max_buffered_records"`
	Throughput         float64 `json:"throughput_cap"`

	Records     int64   `json:"records"`
	Errors      int64   `json:"errors"`
	ElapsedMs   int64   `json:"elapsed_ms"`
	RecsPerSec  float64 `json:"records_per_sec"`
	MBPerSec    float64 `json:"mb_per_sec"`
	AvgLatency  float64 `json:"avg_latency_ms"`
	MaxLatency  float64 `json:"max_latency_ms"`
	P50Latency  int     `json:"p50_latency_ms"`
	P95Latency  int     `json:"p95_latency_ms"`
	P99Latency  int     `json:"p99_latency_ms"`
	P999Latency int     `json:"p999_latency_ms"`
}

type consumeJSON struct {
	Type      string `json:"type"`
	Brokers   string `json:"brokers"`
	Topic     string `json:"topic"`
	Consumers int    `json:"consumers"`
	FetchMax  int    `json:"fetch_max_bytes"`

	Records    int64   `json:"records"`
	Bytes      int64   `json:"bytes"`
	ElapsedMs  int64   `json:"elapsed_ms"`
	RecsPerSec float64 `json:"records_per_sec"`
	MBPerSec   float64 `json:"mb_per_sec"`
}

func writeProducerJSON(path string, cfg bench.ProducerConfig, r *bench.ProducerResult) {
	out := produceJSON{
		Type:        "produce",
		Brokers:     strings.Join(cfg.Brokers, ","),
		Topic:       cfg.Topic,
		Producers:   cfg.NumProducers,
		RecordSize:  cfg.RecordSize,
		Acks:        cfg.Acks,
		BatchMax:           int(cfg.BatchMaxBytes),
		LingerMs:           cfg.LingerMs,
		MaxBufferedRecords: cfg.MaxBufferedRecords,
		Throughput:         cfg.Throughput,
		Records:     r.Records,
		Errors:      r.Errors,
		ElapsedMs:   r.ElapsedMs,
		RecsPerSec:  r.RecsPerSec,
		MBPerSec:    r.MBPerSec,
		AvgLatency:  r.AvgLatency,
		MaxLatency:  r.MaxLatency,
		P50Latency:  r.P50Latency,
		P95Latency:  r.P95Latency,
		P99Latency:  r.P99Latency,
		P999Latency: r.P999Latency,
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	os.WriteFile(path, data, 0644)
}

func writeConsumerJSON(path string, cfg bench.ConsumerConfig, r *bench.ConsumerResult) {
	out := consumeJSON{
		Type:       "consume",
		Brokers:    strings.Join(cfg.Brokers, ","),
		Topic:      cfg.Topic,
		Consumers:  cfg.NumConsumers,
		FetchMax:   int(cfg.FetchMaxBytes),
		Records:    r.Records,
		Bytes:      r.Bytes,
		ElapsedMs:  r.ElapsedMs,
		RecsPerSec: r.RecsPerSec,
		MBPerSec:   r.MBPerSec,
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	os.WriteFile(path, data, 0644)
}
