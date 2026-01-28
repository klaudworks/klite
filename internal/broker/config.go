package broker

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
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

	// WAL configuration (Phase 3)
	WALEnabled         bool   // Enable WAL persistence (default false)
	WALSyncIntervalMs  int    // Fsync batch window in milliseconds (default 2)
	WALSegmentMaxBytes int64  // Max segment size before rotation (default 64 MiB)
	WALMaxDiskSize     int64  // Max total WAL on disk (default 1 GiB)
	RingBufferMaxMem   int64  // Global memory budget for ring buffers (default 512 MiB)

	// S3 configuration (Phase 4)
	S3Bucket        string        // S3 bucket name (empty = S3 disabled)
	S3Region        string        // S3 region
	S3Endpoint      string        // Custom S3 endpoint (for LocalStack/MinIO)
	S3Prefix        string        // S3 key prefix (default: "klite/<clusterID>")
	S3FlushInterval time.Duration // Unified S3 sync interval (default 10m)

	// S3API allows injecting a mock S3 client for tests.
	S3API interface{} // s3.S3API when set

	// Listener allows injecting a pre-created listener (for tests).
	// If non-nil, the broker uses this instead of opening Listen.
	Listener net.Listener
}

// DefaultConfig returns a Config with production defaults.
func DefaultConfig() Config {
	return Config{
		Listen:            ":9092",
		DataDir:           "./data",
		NodeID:            0,
		DefaultPartitions: 1,
		AutoCreateTopics:  true,
		LogLevel:          "info",
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
