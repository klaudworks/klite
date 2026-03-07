package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/klaudworks/klite/internal/broker"
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
