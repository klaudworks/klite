package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/klaudworks/klite/internal/bench"
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
  klite-bench produce [flags]        Run producer throughput/latency test
  klite-bench consume [flags]        Run consumer throughput test
  klite-bench create-topic [flags]   Create a topic
  klite-bench delete-topic [flags]   Delete a topic

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
	fs.Int64Var(&cfg.WarmupRecords, "warmup-records", 0, "Number of initial records to discard from stats")
	reportingMs := fs.Int64("reporting-interval", 5000, "Reporting interval in milliseconds")
	fs.StringVar(&jsonOut, "json-output", "", "Path to write JSON results (optional)")

	fs.Parse(args)

	cfg.Brokers = splitBrokers(brokers)
	cfg.BatchMaxBytes = int32(*batchMax)
	cfg.ReportingInterval = time.Duration(*reportingMs) * time.Millisecond

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := bench.RunProducer(ctx, cfg)

	if jsonOut != "" && result != nil {
		writeProducerJSON(jsonOut, cfg, result)
	}

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
	BatchMax   int     `json:"batch_max_bytes"`
	LingerMs   int     `json:"linger_ms"`
	Throughput float64 `json:"throughput_cap"`

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
		BatchMax:    int(cfg.BatchMaxBytes),
		LingerMs:    cfg.LingerMs,
		Throughput:  cfg.Throughput,
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
