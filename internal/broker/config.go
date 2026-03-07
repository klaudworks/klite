package broker

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// Config holds all broker configuration.
type Config struct {
	Listen            string
	AdvertisedAddr    string
	DataDir           string
	ClusterID         string // empty = auto-generate on first start
	NodeID            int32
	DefaultPartitions int
	AutoCreateTopics  bool
	LogLevel          string
	HealthAddr        string // HTTP health endpoint address (empty = disabled)

	// WAL configuration
	WALSyncIntervalMs  int   // Fsync batch window in milliseconds (default 2)
	WALSegmentMaxBytes int64 // Max segment size before rotation (default 64 MiB)
	WALMaxDiskSize     int64 // Max total WAL on disk (default 4 GiB)
	ChunkPoolMemory    int64 // Global memory budget for chunk pool (default 512 MiB)

	// S3 configuration (Phase 4)
	S3Bucket             string        // S3 bucket name (empty = S3 disabled)
	S3Region             string        // S3 region
	S3Endpoint           string        // Custom S3 endpoint (for LocalStack/MinIO)
	S3Prefix             string        // Optional S3 path prefix prepended before "klite-<clusterID>"
	S3FlushInterval      time.Duration // Max age of unflushed data before flush (default 60s)
	S3TargetObjectSize   int64         // Flush partition when unflushed bytes reach this (default 64 MiB)
	S3FlushCheckInterval time.Duration // How often the flusher scans partitions (default 5s)

	// S3API allows injecting a mock S3 client for tests.
	S3API interface{} // s3.S3API when set

	// SASL configuration (Phase 5)
	SASLEnabled   bool   // Enable SASL authentication
	SASLMechanism string // Mechanism for CLI-specified user: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUser      string // Username for CLI-specified bootstrap user
	SASLPassword  string // Password for CLI-specified bootstrap user

	// Retention configuration (Phase 6)
	RetentionCheckInterval time.Duration // How often the retention goroutine runs (default 1h)

	// Compaction configuration (Phase 6)
	CompactionCheckInterval   time.Duration // How often to scan dirty counters (default 30s)
	CompactionMinDirtyObjects int           // Min dirty objects before compaction triggers (default 4)
	CompactionWindowBytes     int64         // Max source object size per window (default 256 MiB)
	CompactionS3Concurrency   int           // Max concurrent S3 GETs for compaction (default 4)
	CompactionReadRate        int           // Max S3 read bytes/sec for compaction (default 50 MiB/s, 0 = unlimited)

	// SASLStore allows injecting a pre-configured SASL credential store (for tests).
	SASLStore interface{} // *sasl.Store when set

	// Clock allows injecting a controllable clock for tests.
	// If nil, defaults to clock.RealClock{}.
	Clock clock.Clock

	// Listener allows injecting a pre-created listener (for tests).
	// If non-nil, the broker uses this instead of opening Listen.
	Listener net.Listener

	// HealthListener allows injecting a pre-created listener for the HTTP
	// health server (for tests). If non-nil and HealthAddr is set, the
	// broker uses this instead of opening HealthAddr.
	HealthListener net.Listener
}

// DefaultConfig returns a Config with production defaults.
func DefaultConfig() Config {
	return Config{
		Listen:             ":9092",
		DataDir:            "./data",
		NodeID:             0,
		DefaultPartitions:  1,
		AutoCreateTopics:   true,
		LogLevel:           "info",
		CompactionReadRate: 50 * 1024 * 1024, // 50 MiB/s
		// HealthAddr left empty — health server is opt-in via --health-addr.
	}
}

// nodeIDValue implements flag.Value for int32.
type nodeIDValue struct {
	target *int32
}

func (v *nodeIDValue) String() string {
	if v.target == nil {
		return "0"
	}
	return fmt.Sprintf("%d", *v.target)
}

func (v *nodeIDValue) Set(s string) error {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return err
	}
	*v.target = int32(n)
	return nil
}

// RegisterFlags registers CLI flags into the given FlagSet, writing into cfg.
func (cfg *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&cfg.Listen, "listen", cfg.Listen, "Listen address (host:port)")
	fs.StringVar(&cfg.AdvertisedAddr, "advertised-addr", cfg.AdvertisedAddr, "Address clients use to connect (default: derived from --listen)")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory for WAL and metadata")
	fs.StringVar(&cfg.ClusterID, "cluster-id", cfg.ClusterID, "Kafka cluster ID (default: auto-generated UUID)")
	fs.Var(&nodeIDValue{target: &cfg.NodeID}, "node-id", "Broker node ID")
	fs.IntVar(&cfg.DefaultPartitions, "default-partitions", cfg.DefaultPartitions, "Default partition count for auto-created topics")
	fs.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", cfg.AutoCreateTopics, "Auto-create topics on Metadata/Produce")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	fs.StringVar(&cfg.HealthAddr, "health-addr", cfg.HealthAddr, "HTTP health endpoint address (empty = disabled, e.g. :8080)")

	// S3 flags
	fs.StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name (empty = S3 disabled)")
	fs.StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "S3 region (default: us-east-1)")
	fs.StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (for LocalStack/MinIO)")
	fs.StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "Optional S3 path prefix prepended before klite-<clusterID>")
	fs.DurationVar(&cfg.S3FlushInterval, "s3-flush-interval", cfg.S3FlushInterval, "Max age of unflushed partition data before flush (default: 60s)")
	fs.Int64Var(&cfg.S3TargetObjectSize, "s3-target-object-size", cfg.S3TargetObjectSize, "Flush partition when unflushed bytes reach this size in bytes (default: 67108864 = 64 MiB)")
	fs.DurationVar(&cfg.S3FlushCheckInterval, "s3-flush-check-interval", cfg.S3FlushCheckInterval, "How often the flusher scans partitions for flush eligibility (default: 1s)")

	// WAL flags
	fs.IntVar(&cfg.WALSyncIntervalMs, "wal-sync-interval", cfg.WALSyncIntervalMs, "WAL fsync batch window in milliseconds (default: 2)")
	fs.Int64Var(&cfg.WALSegmentMaxBytes, "wal-segment-max-bytes", cfg.WALSegmentMaxBytes, "Max WAL segment size before rotation in bytes (default: 67108864 = 64 MiB)")
	fs.Int64Var(&cfg.WALMaxDiskSize, "wal-max-disk-size", cfg.WALMaxDiskSize, "Max total WAL on disk in bytes (default: 4 GiB)")
	fs.Int64Var(&cfg.ChunkPoolMemory, "chunk-pool-memory", cfg.ChunkPoolMemory, "Global memory budget for chunk pool in bytes (default: 536870912 = 512 MiB)")

	// Retention flags
	fs.DurationVar(&cfg.RetentionCheckInterval, "retention-check-interval", cfg.RetentionCheckInterval, "How often the retention goroutine runs (default: 1h)")

	// Compaction flags
	fs.DurationVar(&cfg.CompactionCheckInterval, "compaction-check-interval", cfg.CompactionCheckInterval, "How often to scan dirty counters for eligible partitions (default: 30s)")
	fs.IntVar(&cfg.CompactionMinDirtyObjects, "compaction-min-dirty-objects", cfg.CompactionMinDirtyObjects, "Min dirty S3 objects before compaction triggers (default: 4)")
	fs.IntVar(&cfg.CompactionReadRate, "compaction-read-rate", cfg.CompactionReadRate, "Max S3 read bytes/sec for compaction (default: 52428800 = 50 MiB/s, 0 = unlimited)")

	// SASL flags
	fs.BoolVar(&cfg.SASLEnabled, "sasl-enabled", cfg.SASLEnabled, "Enable SASL authentication")
	fs.StringVar(&cfg.SASLMechanism, "sasl-mechanism", cfg.SASLMechanism, "Mechanism for CLI-specified user: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
	fs.StringVar(&cfg.SASLUser, "sasl-user", cfg.SASLUser, "Username for CLI-specified bootstrap user")
	fs.StringVar(&cfg.SASLPassword, "sasl-password", cfg.SASLPassword, "Password for CLI-specified bootstrap user")
}

// ApplyEnvOverrides reads KLITE_* environment variables and applies them to
// fields that haven't been explicitly set via flags. Call after flag.Parse().
func (cfg *Config) ApplyEnvOverrides() {
	envOverrides := []struct {
		env    string
		target *string
	}{
		{"KLITE_LISTEN", &cfg.Listen},
		{"KLITE_ADVERTISED_ADDR", &cfg.AdvertisedAddr},
		{"KLITE_DATA_DIR", &cfg.DataDir},
		{"KLITE_CLUSTER_ID", &cfg.ClusterID},
		{"KLITE_LOG_LEVEL", &cfg.LogLevel},
		{"KLITE_HEALTH_ADDR", &cfg.HealthAddr},
	}
	for _, eo := range envOverrides {
		if v := os.Getenv(eo.env); v != "" {
			*eo.target = v
		}
	}

	if v := os.Getenv("KLITE_NODE_ID"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			cfg.NodeID = int32(n)
		}
	}
	if v := os.Getenv("KLITE_DEFAULT_PARTITIONS"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			cfg.DefaultPartitions = n
		}
	}
	if v := os.Getenv("KLITE_AUTO_CREATE_TOPICS"); v != "" {
		cfg.AutoCreateTopics = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("KLITE_WAL_MAX_DISK_SIZE"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			cfg.WALMaxDiskSize = n
		}
	}
}

// ResolveAdvertisedAddr determines the advertised address from config.
// Returns the address and whether a warning should be logged.
func (cfg *Config) ResolveAdvertisedAddr() (addr string, warn bool) {
	if cfg.AdvertisedAddr != "" {
		return cfg.AdvertisedAddr, false
	}
	host, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return cfg.Listen, false
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return cfg.Listen, false
	}
	return net.JoinHostPort("localhost", port), true
}
