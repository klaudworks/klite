package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/klaudworks/klite/internal/broker"
)

func main() {
	cfg := broker.DefaultConfig()

	fs := flag.CommandLine
	cfg.RegisterFlags(fs)
	fs.Parse(os.Args[1:])
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
