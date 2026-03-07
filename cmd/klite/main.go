package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/klaudworks/klite/internal/broker"
	"github.com/klaudworks/klite/internal/k8s"
)

// Set via -ldflags at build time.
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")

	cfg := broker.DefaultConfig()

	fs := flag.CommandLine
	cfg.RegisterFlags(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		slog.Error("failed to parse flags", "error", err)
		os.Exit(1)
	}

	if *showVersion {
		fmt.Println("klite", version)
		os.Exit(0)
	}

	cfg.ApplyEnvOverrides()

	if cfg.ReplicationAddr != "" {
		podName := os.Getenv("POD_NAME")

		// Derive node ID from StatefulSet pod ordinal (e.g. "klite-1" → 1).
		// Only applies when node ID wasn't explicitly set via flag or env.
		if podName != "" && cfg.NodeID == 0 {
			if idx := strings.LastIndex(podName, "-"); idx >= 0 {
				if ordinal, err := strconv.ParseInt(podName[idx+1:], 10, 32); err == nil {
					cfg.NodeID = int32(ordinal)
					slog.Info("node-id derived from pod ordinal", "pod", podName, "node-id", cfg.NodeID)
				}
			}
		}

		// In Kubernetes with replication, patch the pod label on role changes
		// so the Service routes traffic only to the primary.
		// Outside k8s (no service account token), this is a no-op.
		labeler := k8s.NewPodLabeler(
			podName,
			os.Getenv("POD_NAMESPACE"),
			os.Getenv("KLITE_LABEL_SELECTOR"),
			slog.Default(),
		)
		if labeler != nil {
			cfg.RoleChangeHook = labeler
		} else {
			slog.Warn("pod labeler disabled: not running in Kubernetes or POD_NAME not set; Service routing labels will not be managed")
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)

	b := broker.New(cfg)
	if err := b.Run(ctx); err != nil {
		stop()
		slog.Error("broker failed", "error", err)
		os.Exit(1)
	}
	stop()
}
