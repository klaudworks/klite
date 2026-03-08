package broker

import (
	"fmt"
	"net"
	"time"

	"github.com/klaudworks/klite/internal/clock"
)

// RoleHook is called when the broker's replication role changes.
type RoleHook interface {
	SetPrimary()
	ClearPrimary()
}

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

	// Replication configuration
	ReplicationAddr           string        // Listen address for replication (e.g. ":9093"). Setting enables standby mode.
	ReplicationAdvertisedAddr string        // Advertised replication address for standby discovery
	ReplicationID             string        // Node identity for lease (default: hostname)
	ReplicationTLSSANs        []string      // Extra DNS SANs for the replication TLS certificate
	ReplicationAckTimeout     time.Duration // Timeout for standby ACK in sync mode (default 5s)
	LeaseDuration             time.Duration // How long the lease is valid without renewal (default 15s)
	LeaseRenewInterval        time.Duration // How often the primary renews the lease (default 5s)
	LeaseRetryInterval        time.Duration // How often the standby polls the lease (default 2s)

	// RoleChangeHook is called on primary promotion/demotion.
	// Used by the k8s pod labeler to update Service routing labels.
	RoleChangeHook RoleHook

	// LeaseElector allows injecting a lease.Elector for tests.
	LeaseElector interface{} // lease.Elector when set

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

// ValidateReplication checks replication configuration constraints.
// Returns an error for fatal misconfigurations, and a list of warnings.
func (cfg *Config) ValidateReplication() (warnings []string, err error) {
	if cfg.ReplicationAddr == "" {
		return nil, nil
	}

	if cfg.S3Bucket == "" && cfg.LeaseElector == nil {
		return nil, fmt.Errorf("--replication-addr requires --s3-bucket (S3 is needed for lease fencing and TLS cert storage)")
	}

	leaseDur := cfg.LeaseDuration
	if leaseDur == 0 {
		leaseDur = 15 * time.Second
	}
	renewInt := cfg.LeaseRenewInterval
	if renewInt == 0 {
		renewInt = 5 * time.Second
	}
	retryInt := cfg.LeaseRetryInterval
	if retryInt == 0 {
		retryInt = 2 * time.Second
	}

	if leaseDur < 2*renewInt {
		return nil, fmt.Errorf("--lease-duration (%s) must be >= 2x --lease-renew-interval (%s)", leaseDur, renewInt)
	}

	if retryInt > leaseDur {
		warnings = append(warnings, fmt.Sprintf("--lease-retry-interval (%s) > --lease-duration (%s): standby may miss the expiry window", retryInt, leaseDur))
	}

	return warnings, nil
}

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
