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
	fs.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println("klite", version)
		os.Exit(0)
	}

	cfg.ApplyEnvOverrides()

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	b := broker.New(cfg)
	if err := b.Run(ctx); err != nil {
		slog.Error("broker failed", "error", err)
		os.Exit(1)
	}
}
