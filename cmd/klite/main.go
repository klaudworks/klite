package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	cli "github.com/urfave/cli/v3"

	"github.com/klaudworks/klite/internal/broker"
	"github.com/klaudworks/klite/internal/k8s"
)

// Set via -ldflags at build time.
var version = "dev"

func main() {
	cfg := broker.DefaultConfig()

	root := &cli.Command{
		Name:    "klite",
		Usage:   "Kafka-compatible broker in a single binary",
		Version: version,
		Flags: []cli.Flag{
			// Core
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "Listen address (host:port)",
				Value:       cfg.Listen,
				Destination: &cfg.Listen,
				Sources:     cli.EnvVars("KLITE_LISTEN"),
			},
			&cli.StringFlag{
				Name:        "advertised-addr",
				Usage:       "Address clients use to connect (default: derived from --listen)",
				Destination: &cfg.AdvertisedAddr,
				Sources:     cli.EnvVars("KLITE_ADVERTISED_ADDR"),
			},
			&cli.StringFlag{
				Name:        "data-dir",
				Usage:       "Data directory for WAL and metadata",
				Value:       cfg.DataDir,
				Destination: &cfg.DataDir,
				Sources:     cli.EnvVars("KLITE_DATA_DIR"),
			},
			&cli.StringFlag{
				Name:        "cluster-id",
				Usage:       "Kafka cluster ID (default: auto-generated UUID)",
				Destination: &cfg.ClusterID,
				Sources:     cli.EnvVars("KLITE_CLUSTER_ID"),
			},
			&cli.IntFlag{
				Name:    "node-id",
				Usage:   "Broker node ID",
				Value:   int(cfg.NodeID),
				Sources: cli.EnvVars("KLITE_NODE_ID"),
			},
			&cli.IntFlag{
				Name:        "default-partitions",
				Usage:       "Default partition count for auto-created topics",
				Value:       cfg.DefaultPartitions,
				Destination: &cfg.DefaultPartitions,
				Sources:     cli.EnvVars("KLITE_DEFAULT_PARTITIONS"),
			},
			&cli.BoolFlag{
				Name:        "auto-create-topics",
				Usage:       "Auto-create topics on Metadata/Produce",
				Value:       cfg.AutoCreateTopics,
				Destination: &cfg.AutoCreateTopics,
				Sources:     cli.EnvVars("KLITE_AUTO_CREATE_TOPICS"),
			},
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "Log level: debug, info, warn, error",
				Value:       cfg.LogLevel,
				Destination: &cfg.LogLevel,
				Sources:     cli.EnvVars("KLITE_LOG_LEVEL"),
			},
			&cli.StringFlag{
				Name:        "health-addr",
				Usage:       "HTTP health endpoint address (empty = disabled, e.g. :8080)",
				Destination: &cfg.HealthAddr,
				Sources:     cli.EnvVars("KLITE_HEALTH_ADDR"),
			},

			// WAL
			&cli.IntFlag{
				Name:        "wal-sync-interval",
				Usage:       "WAL fsync batch window in milliseconds (default: 2)",
				Destination: &cfg.WALSyncIntervalMs,
			},
			&cli.Int64Flag{
				Name:        "wal-segment-max-bytes",
				Usage:       "Max WAL segment size before rotation in bytes (default: 64 MiB)",
				Destination: &cfg.WALSegmentMaxBytes,
			},
			&cli.Int64Flag{
				Name:        "wal-max-disk-size",
				Usage:       "Max total WAL on disk in bytes (default: 4 GiB)",
				Destination: &cfg.WALMaxDiskSize,
				Sources:     cli.EnvVars("KLITE_WAL_MAX_DISK_SIZE"),
			},
			&cli.Int64Flag{
				Name:        "chunk-pool-memory",
				Usage:       "Global memory budget for chunk pool in bytes (default: 512 MiB)",
				Destination: &cfg.ChunkPoolMemory,
			},

			// S3
			&cli.StringFlag{
				Name:        "s3-bucket",
				Usage:       "S3 bucket name (empty = S3 disabled)",
				Destination: &cfg.S3Bucket,
			},
			&cli.StringFlag{
				Name:        "s3-region",
				Usage:       "S3 region",
				Destination: &cfg.S3Region,
			},
			&cli.StringFlag{
				Name:        "s3-endpoint",
				Usage:       "Custom S3 endpoint (for LocalStack/MinIO)",
				Destination: &cfg.S3Endpoint,
			},
			&cli.StringFlag{
				Name:        "s3-prefix",
				Usage:       "Optional S3 path prefix prepended before klite-<clusterID>",
				Destination: &cfg.S3Prefix,
			},
			&cli.DurationFlag{
				Name:        "s3-flush-interval",
				Usage:       "Max age of unflushed partition data before flush (default: 60s)",
				Destination: &cfg.S3FlushInterval,
			},
			&cli.Int64Flag{
				Name:        "s3-target-object-size",
				Usage:       "Flush partition when unflushed bytes reach this size (default: 64 MiB)",
				Destination: &cfg.S3TargetObjectSize,
			},
			&cli.DurationFlag{
				Name:        "s3-flush-check-interval",
				Usage:       "How often the flusher scans partitions (default: 1s)",
				Destination: &cfg.S3FlushCheckInterval,
			},

			// Retention
			&cli.DurationFlag{
				Name:        "retention-check-interval",
				Usage:       "How often the retention goroutine runs (default: 1h)",
				Destination: &cfg.RetentionCheckInterval,
			},

			// Compaction
			&cli.DurationFlag{
				Name:        "compaction-check-interval",
				Usage:       "How often to scan dirty counters (default: 30s)",
				Destination: &cfg.CompactionCheckInterval,
			},
			&cli.IntFlag{
				Name:        "compaction-min-dirty-objects",
				Usage:       "Min dirty S3 objects before compaction triggers (default: 4)",
				Destination: &cfg.CompactionMinDirtyObjects,
			},
			&cli.IntFlag{
				Name:        "compaction-read-rate",
				Usage:       "Max S3 read bytes/sec for compaction (default: 50 MiB/s, 0 = unlimited)",
				Value:       cfg.CompactionReadRate,
				Destination: &cfg.CompactionReadRate,
			},

			// SASL
			&cli.BoolFlag{
				Name:        "sasl-enabled",
				Usage:       "Enable SASL authentication",
				Destination: &cfg.SASLEnabled,
			},
			&cli.StringFlag{
				Name:        "sasl-mechanism",
				Usage:       "Mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
				Destination: &cfg.SASLMechanism,
			},
			&cli.StringFlag{
				Name:        "sasl-user",
				Usage:       "Username for CLI-specified bootstrap user",
				Destination: &cfg.SASLUser,
			},
			&cli.StringFlag{
				Name:        "sasl-password",
				Usage:       "Password for CLI-specified bootstrap user",
				Destination: &cfg.SASLPassword,
			},

			// Replication
			&cli.StringFlag{
				Name:        "replication-addr",
				Usage:       "Replication listen address. Setting this enables standby mode.",
				Destination: &cfg.ReplicationAddr,
				Sources:     cli.EnvVars("KLITE_REPLICATION_ADDR"),
			},
			&cli.StringFlag{
				Name:        "replication-advertised-addr",
				Usage:       "Routable replication address written to the lease",
				Destination: &cfg.ReplicationAdvertisedAddr,
				Sources:     cli.EnvVars("KLITE_REPLICATION_ADVERTISED_ADDR"),
			},
			&cli.StringFlag{
				Name:        "replication-id",
				Usage:       "Node identity for lease (default: hostname)",
				Destination: &cfg.ReplicationID,
				Sources:     cli.EnvVars("KLITE_REPLICATION_ID"),
			},
			&cli.StringSliceFlag{
				Name:  "replication-tls-san",
				Usage: "Additional DNS SAN for the replication TLS certificate (can be repeated)",
			},
			&cli.DurationFlag{
				Name:        "replication-ack-timeout",
				Usage:       "Timeout for standby ACK in sync mode (default: 5s)",
				Destination: &cfg.ReplicationAckTimeout,
				Sources:     cli.EnvVars("KLITE_REPLICATION_ACK_TIMEOUT"),
			},
			&cli.DurationFlag{
				Name:        "lease-duration",
				Usage:       "How long the lease is valid without renewal (default: 15s)",
				Destination: &cfg.LeaseDuration,
				Sources:     cli.EnvVars("KLITE_LEASE_DURATION"),
			},
			&cli.DurationFlag{
				Name:        "lease-renew-interval",
				Usage:       "How often the primary renews the lease (default: 5s)",
				Destination: &cfg.LeaseRenewInterval,
				Sources:     cli.EnvVars("KLITE_LEASE_RENEW_INTERVAL"),
			},
			&cli.DurationFlag{
				Name:        "lease-retry-interval",
				Usage:       "How often the standby polls the lease (default: 2s)",
				Destination: &cfg.LeaseRetryInterval,
				Sources:     cli.EnvVars("KLITE_LEASE_RETRY_INTERVAL"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// Bind flags that need type conversion.
			cfg.NodeID = int32(cmd.Int("node-id"))
			cfg.ReplicationTLSSANs = cmd.StringSlice("replication-tls-san")

			if cfg.ReplicationAddr != "" {
				podName := os.Getenv("POD_NAME")

				// Derive node ID from StatefulSet pod ordinal (e.g. "klite-1" -> 1).
				if podName != "" && cfg.NodeID == 0 {
					if idx := strings.LastIndex(podName, "-"); idx >= 0 {
						if ordinal, err := strconv.ParseInt(podName[idx+1:], 10, 32); err == nil {
							cfg.NodeID = int32(ordinal)
							slog.Info("node-id derived from pod ordinal", "pod", podName, "node-id", cfg.NodeID)
						}
					}
				}

				labeler := k8s.NewPodLabeler(
					podName,
					os.Getenv("POD_NAMESPACE"),
					os.Getenv("KLITE_LABEL_SELECTOR"),
					slog.Default(),
				)
				if labeler != nil {
					cfg.RoleChangeHook = labeler
				} else {
					slog.Warn("pod labeler disabled: not running in Kubernetes or POD_NAME not set")
				}
			}

			sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			b := broker.New(cfg)
			if err := b.Run(sigCtx); err != nil {
				stop()
				return fmt.Errorf("broker failed: %w", err)
			}
			stop()
			return nil
		},
	}

	if err := root.Run(context.Background(), os.Args); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
