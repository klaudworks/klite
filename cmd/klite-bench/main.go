package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	cli "github.com/urfave/cli/v3"

	"github.com/klaudworks/klite/internal/bench"
	s3fmt "github.com/klaudworks/klite/internal/s3"
)

func main() {
	root := &cli.Command{
		Name:  "klite-bench",
		Usage: "Kafka-compatible performance testing tool",
		Commands: []*cli.Command{
			produceCmd(),
			consumeCmd(),
			produceConsumeCmd(),
			createTopicCmd(),
			deleteTopicCmd(),
			s3CountCmd(),
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	if err := root.Run(ctx, os.Args); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	cancel()
}

func produceCmd() *cli.Command {
	return &cli.Command{
		Name:  "produce",
		Usage: "Run producer throughput/latency test",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bootstrap-server", Value: "localhost:9092", Usage: "Broker address(es), comma-separated"},
			&cli.StringFlag{Name: "topic", Value: "bench", Usage: "Topic to produce to"},
			&cli.Int64Flag{Name: "num-records", Value: 1_000_000, Usage: "Number of records to produce"},
			&cli.IntFlag{Name: "record-size", Value: 1000, Usage: "Record size in bytes"},
			&cli.Float64Flag{Name: "throughput", Value: -1, Usage: "Records/sec cap (-1 = unlimited)"},
			&cli.IntFlag{Name: "acks", Value: 1, Usage: "Required acks: -1 (all), 0, 1"},
			&cli.IntFlag{Name: "batch-max-bytes", Value: 1_048_576, Usage: "Max bytes per batch"},
			&cli.IntFlag{Name: "linger-ms", Value: 0, Usage: "Producer linger in milliseconds"},
			&cli.IntFlag{Name: "producers", Value: 1, Usage: "Number of concurrent producer clients"},
			&cli.IntFlag{Name: "max-buffered-records", Value: 8192, Usage: "Max records buffered in client (controls backpressure)"},
			&cli.Int64Flag{Name: "warmup-records", Value: 0, Usage: "Number of initial records to discard from stats"},
			&cli.Int64Flag{Name: "reporting-interval", Value: 5000, Usage: "Reporting interval in milliseconds"},
			&cli.StringFlag{Name: "json-output", Usage: "Path to write JSON results (optional)"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg := bench.DefaultProducerConfig()
			cfg.Brokers = splitBrokers(cmd.String("bootstrap-server"))
			cfg.Topic = cmd.String("topic")
			cfg.NumRecords = cmd.Int64("num-records")
			cfg.RecordSize = cmd.Int("record-size")
			cfg.Throughput = cmd.Float64("throughput")
			cfg.Acks = cmd.Int("acks")
			cfg.BatchMaxBytes = int32(cmd.Int("batch-max-bytes"))
			cfg.LingerMs = cmd.Int("linger-ms")
			cfg.NumProducers = cmd.Int("producers")
			cfg.MaxBufferedRecords = cmd.Int("max-buffered-records")
			cfg.WarmupRecords = cmd.Int64("warmup-records")
			cfg.ReportingInterval = time.Duration(cmd.Int64("reporting-interval")) * time.Millisecond

			if path := cmd.String("json-output"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("creating json output file: %w", err)
				}
				defer f.Close() //nolint:errcheck
				cfg.JSONOut = f
			}

			_, err := bench.RunProducer(ctx, cfg)
			return err
		},
	}
}

func consumeCmd() *cli.Command {
	return &cli.Command{
		Name:  "consume",
		Usage: "Run consumer throughput test",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bootstrap-server", Value: "localhost:9092", Usage: "Broker address(es), comma-separated"},
			&cli.StringFlag{Name: "topic", Value: "bench", Usage: "Topic to consume from"},
			&cli.Int64Flag{Name: "num-records", Value: 1_000_000, Usage: "Number of records to consume"},
			&cli.IntFlag{Name: "fetch-max-bytes", Value: 1_048_576, Usage: "Max fetch bytes per partition"},
			&cli.IntFlag{Name: "consumers", Value: 1, Usage: "Number of concurrent consumers in the same group"},
			&cli.Int64Flag{Name: "timeout", Value: 60000, Usage: "Max time in ms between received records before exiting"},
			&cli.Int64Flag{Name: "reporting-interval", Value: 5000, Usage: "Reporting interval in milliseconds"},
			&cli.StringFlag{Name: "json-output", Usage: "Path to write JSON Lines results (optional)"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg := bench.DefaultConsumerConfig()
			cfg.Brokers = splitBrokers(cmd.String("bootstrap-server"))
			cfg.Topic = cmd.String("topic")
			cfg.NumRecords = cmd.Int64("num-records")
			cfg.FetchMaxBytes = int32(cmd.Int("fetch-max-bytes"))
			cfg.NumConsumers = cmd.Int("consumers")
			cfg.Timeout = time.Duration(cmd.Int64("timeout")) * time.Millisecond
			cfg.ReportingInterval = time.Duration(cmd.Int64("reporting-interval")) * time.Millisecond

			if path := cmd.String("json-output"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("creating json output file: %w", err)
				}
				defer f.Close() //nolint:errcheck
				cfg.JSONOut = f
			}

			_, err := bench.RunConsumer(ctx, cfg)
			return err
		},
	}
}

func produceConsumeCmd() *cli.Command {
	return &cli.Command{
		Name:  "produce-consume",
		Usage: "Run producers and consumers in parallel, measure e2e latency",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bootstrap-server", Value: "localhost:9092", Usage: "Broker address(es), comma-separated"},
			&cli.StringFlag{Name: "topic", Value: "bench", Usage: "Topic to produce to and consume from"},
			&cli.Int64Flag{Name: "num-records", Value: 1_000_000, Usage: "Number of records to produce"},
			&cli.IntFlag{Name: "record-size", Value: 1000, Usage: "Record value size in bytes"},
			&cli.Float64Flag{Name: "throughput", Value: -1, Usage: "Records/sec cap (-1 = unlimited)"},
			&cli.IntFlag{Name: "acks", Value: 1, Usage: "Required acks: -1 (all), 0, 1"},
			&cli.IntFlag{Name: "batch-max-bytes", Value: 1_048_576, Usage: "Max bytes per batch"},
			&cli.IntFlag{Name: "linger-ms", Value: 0, Usage: "Producer linger in milliseconds"},
			&cli.IntFlag{Name: "producers", Value: 1, Usage: "Number of concurrent producers"},
			&cli.IntFlag{Name: "consumers", Value: 1, Usage: "Number of concurrent consumers"},
			&cli.IntFlag{Name: "max-buffered-records", Value: 8192, Usage: "Max records buffered in client"},
			&cli.Int64Flag{Name: "warmup-records", Value: 0, Usage: "Records to discard from stats"},
			&cli.Int64Flag{Name: "reporting-interval", Value: 5000, Usage: "Reporting interval in milliseconds"},
			&cli.Int64Flag{Name: "drain-timeout", Value: 30000, Usage: "Max ms to wait for consumers after produce finishes"},
			&cli.StringFlag{Name: "json-output", Usage: "Path to write JSON Lines results"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg := bench.DefaultProduceConsumeConfig()
			cfg.Brokers = splitBrokers(cmd.String("bootstrap-server"))
			cfg.Topic = cmd.String("topic")
			cfg.NumRecords = cmd.Int64("num-records")
			cfg.RecordSize = cmd.Int("record-size")
			cfg.Throughput = cmd.Float64("throughput")
			cfg.Acks = cmd.Int("acks")
			cfg.BatchMaxBytes = int32(cmd.Int("batch-max-bytes"))
			cfg.LingerMs = cmd.Int("linger-ms")
			cfg.NumProducers = cmd.Int("producers")
			cfg.NumConsumers = cmd.Int("consumers")
			cfg.MaxBufferedRecords = cmd.Int("max-buffered-records")
			cfg.WarmupRecords = cmd.Int64("warmup-records")
			cfg.ReportingInterval = time.Duration(cmd.Int64("reporting-interval")) * time.Millisecond
			cfg.DrainTimeout = time.Duration(cmd.Int64("drain-timeout")) * time.Millisecond

			if path := cmd.String("json-output"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("creating json output file: %w", err)
				}
				defer f.Close() //nolint:errcheck
				cfg.JSONOut = f
			}

			_, err := bench.RunProduceConsume(ctx, cfg)
			return err
		},
	}
}

func createTopicCmd() *cli.Command {
	return &cli.Command{
		Name:  "create-topic",
		Usage: "Create a topic",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bootstrap-server", Value: "localhost:9092", Usage: "Broker address(es), comma-separated"},
			&cli.StringFlag{Name: "topic", Value: "bench", Usage: "Topic to create"},
			&cli.IntFlag{Name: "partitions", Value: 6, Usage: "Number of partitions"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return bench.CreateTopic(ctx,
				splitBrokers(cmd.String("bootstrap-server")),
				cmd.String("topic"),
				int32(cmd.Int("partitions")),
			)
		},
	}
}

func deleteTopicCmd() *cli.Command {
	return &cli.Command{
		Name:  "delete-topic",
		Usage: "Delete a topic",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bootstrap-server", Value: "localhost:9092", Usage: "Broker address(es), comma-separated"},
			&cli.StringFlag{Name: "topic", Value: "bench", Usage: "Topic to delete"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return bench.DeleteTopic(ctx,
				splitBrokers(cmd.String("bootstrap-server")),
				cmd.String("topic"),
			)
		},
	}
}

func s3CountCmd() *cli.Command {
	return &cli.Command{
		Name:  "s3-count",
		Usage: "Count records in S3 objects",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bucket", Required: true, Usage: "S3 bucket name"},
			&cli.StringFlag{Name: "prefix", Usage: "S3 key prefix (e.g. <run_id>/)"},
			&cli.StringFlag{Name: "region", Value: "eu-west-1", Usage: "AWS region"},
			&cli.StringFlag{Name: "endpoint", Usage: "S3 endpoint URL (for LocalStack, MinIO, etc.)"},
			&cli.BoolFlag{Name: "json", Usage: "Output as JSON"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			bucket := cmd.String("bucket")
			prefix := cmd.String("prefix")
			region := cmd.String("region")
			endpoint := cmd.String("endpoint")
			jsonOutput := cmd.Bool("json")

			cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
			if err != nil {
				return fmt.Errorf("loading AWS config: %w", err)
			}
			var s3Opts []func(*s3.Options)
			if endpoint != "" {
				s3Opts = append(s3Opts, func(o *s3.Options) {
					o.BaseEndpoint = &endpoint
					o.UsePathStyle = true
				})
			}
			client := s3.NewFromConfig(cfg, s3Opts...)

			type objEntry struct {
				Key  string
				Size int64
			}
			var objects []objEntry
			paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: &bucket,
				Prefix: aws.String(prefix),
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

			var dataObjects []objEntry
			for _, obj := range objects {
				if !strings.HasSuffix(obj.Key, "metadata.log") {
					dataObjects = append(dataObjects, obj)
				}
			}

			type footerResult struct {
				key     string
				records int64
				err     error
			}

			const workers = 64
			work := make(chan objEntry, len(dataObjects))
			results := make(chan footerResult, len(dataObjects))

			for range workers {
				go func() {
					for obj := range work {
						footer, err := fetchFooter(ctx, client, bucket, obj.Key, obj.Size)
						if err != nil {
							results <- footerResult{key: obj.Key, err: err}
							continue
						}
						results <- footerResult{key: obj.Key, records: footer.TotalRecordCount()}
					}
				}()
			}

			for _, obj := range dataObjects {
				work <- obj
			}
			close(work)

			var totalRecords int64
			var totalObjects int
			partitionRecords := map[string]int64{}

			for range dataObjects {
				r := <-results
				if r.err != nil {
					fmt.Fprintf(os.Stderr, "WARN: %s: %v\n", r.key, r.err)
					continue
				}
				totalRecords += r.records
				totalObjects++

				parts := strings.Split(r.key, "/")
				if len(parts) >= 2 {
					partKey := parts[len(parts)-2]
					partitionRecords[partKey] += r.records
				}
			}

			if jsonOutput {
				out := s3CountJSON{
					Bucket:     bucket,
					Prefix:     prefix,
					Objects:    totalObjects,
					Records:    totalRecords,
					Partitions: partitionRecords,
				}
				data, _ := json.Marshal(out)
				fmt.Println(string(data))
			} else {
				fmt.Printf("Bucket:  s3://%s/%s\n", bucket, prefix)
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
		},
	}
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
	defer resp.Body.Close() //nolint:errcheck
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
