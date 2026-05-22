// Package main is the entrypoint for the TorBox Media Center FUSE filesystem.
// It wires together config, state, the TorBox API client, catalog, stream reader,
// cache, metrics, and the FUSE mount into a single long-running process.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/iiieii/torbox-fuse-go/internal/cache"
	"github.com/iiieii/torbox-fuse-go/internal/catalog"
	"github.com/iiieii/torbox-fuse-go/internal/config"
	"github.com/iiieii/torbox-fuse-go/internal/fusefs"
	"github.com/iiieii/torbox-fuse-go/internal/metrics"
	"github.com/iiieii/torbox-fuse-go/internal/state"
	"github.com/iiieii/torbox-fuse-go/internal/stream"
	"github.com/iiieii/torbox-fuse-go/internal/torbox"
)

func main() {
	// Load configuration from environment.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	// Set up structured logging with configurable level.
	if err := setupLogger(cfg.LogLevel); err != nil {
		slog.Error("setup logger", "err", err)
		os.Exit(1)
	}

	slog.Info("torbox-media-center starting",
		"mount", cfg.MountPath,
		"cache_mb", cfg.CacheBudgetMB,
		"refresh_interval", cfg.CatalogRefreshInterval,
		"metrics_addr", cfg.MetricsListenAddr,
	)

	// Open state database.
	stateDB, err := state.Open(cfg.StateDBPath)
	if err != nil {
		slog.Error("open state db", "path", cfg.StateDBPath, "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := stateDB.Close(); err != nil {
			slog.Error("close state db", "err", err)
		}
	}()

	// Create metrics collector.
	m := metrics.New()

	// Create TorBox API client.
	tbClient := torbox.NewClient(cfg.TorboxConfig())

	// Create catalog.
	cat := catalog.NewCatalog(tbClient, stateDB, m)

	// Create range cache with configured budget.
	budgetBytes := int64(cfg.CacheBudgetMB) * 1024 * 1024
	rc := cache.NewRangeCache(budgetBytes)

	// Create CDN client with configured concurrency.
	cdn := stream.NewCDNClient(cfg.StreamConcurrency)

	// Create permalink builder from config and TorBox client.
	permalinkBuilder := fusefs.PermalinkBuilderFromClient(cfg, tbClient)

	// Create stream reader.
	prefetchBytes := int64(cfg.PrefetchWindowMB) * 1024 * 1024
	streamer := stream.NewStreamReader(rc, cdn, cfg.StreamMaxInflight, prefetchBytes, permalinkBuilder)

	// Initial catalog refresh.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slog.Info("running initial catalog refresh")
	if err := cat.Refresh(ctx); err != nil {
		slog.Error("initial catalog refresh", "err", err)
		os.Exit(1)
	}

	// Create FUSE root node from the current catalog tree.
	root := fusefs.NewRootNode(cat.Tree(), stateDB, streamer, cfg, tbClient)

	// Set up metrics server with refresh handler.
	metricsServer := metrics.NewServer(m, cfg.MetricsListenAddr, cat.Refresh)

	// Start metrics HTTP server.
	if err := metricsServer.Start(); err != nil {
		slog.Error("start metrics server", "err", err)
		os.Exit(1)
	}

	// Mount FUSE filesystem in a goroutine.
	mountErr := make(chan error, 1)
	go func() {
		slog.Info("mounting FUSE filesystem", "path", cfg.MountPath)
		if err := fusefs.Mount(ctx, cfg.MountPath, root, cfg); err != nil {
			mountErr <- err
		}
		close(mountErr)
	}()

	// Periodic catalog refresh.
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		ticker := time.NewTicker(cfg.CatalogRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				slog.Info("periodic catalog refresh")
				if err := cat.Refresh(ctx); err != nil {
					slog.Error("periodic catalog refresh", "err", err)
				}
			}
		}
	}()

	// Wait for SIGINT or SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("received signal, shutting down", "signal", sig)
	case err := <-mountErr:
		if err != nil {
			slog.Error("FUSE mount error", "err", err)
		}
	}

	// Cancel context to trigger unmount and stop refresh ticker.
	cancel()

	// Shutdown metrics server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown", "err", err)
	}

	// Wait for periodic refresh goroutine to finish.
	<-refreshDone

	// Wait for FUSE mount goroutine to finish (it will unmount via context cancellation).
	<-mountErr

	slog.Info("torbox-media-center stopped")
}

// setupLogger configures slog with the given log level string.
// Valid values: debug, info, warn, error. Defaults to info.
func setupLogger(level string) error {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		// Try parsing as integer for numeric levels.
		n, err := strconv.Atoi(level)
		if err != nil {
			lvl = slog.LevelInfo
		} else {
			lvl = slog.Level(n)
		}
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(handler))
	return nil
}