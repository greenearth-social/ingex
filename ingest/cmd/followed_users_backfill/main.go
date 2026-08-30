package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/followed_users_backfill"
)

func main() {
	concurrency := flag.Int("concurrency", 10, "Concurrent Bluesky walks in flight")
	maxFollowedUsers := flag.Int("max-followed-users", 1_000, "Cap on follows fetched and stored per user")
	retentionDays := flag.Int("retention-days", 30, "Firestore native-TTL retention for cache entries")
	perUserTimeoutSec := flag.Int("per-user-timeout-sec", 15, "Budget for one user's Bluesky walk")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)
	logger.SetDebugEnabled(*debug)

	otelCollector, err := common.NewOTelMetricCollector("followed-users-backfill", config.Environment, config.GCPProjectID, config.GCPRegion, config.MetricExportIntervalSec)
	if err != nil {
		logger.Error("Failed to create OTel metric collector: %v (continuing without metrics)", err)
	} else {
		logger.SetMetricCollector(otelCollector)
		defer func() {
			if err := otelCollector.Shutdown(context.Background()); err != nil {
				logger.Error("Failed to shutdown OTel metric collector: %v", err)
			}
		}()
	}

	logger.Info("Green Earth Ingex - Followed-Users Backfill")

	if !config.FollowCacheEnabled() {
		logger.Error("GE_FIRESTORE_PROJECT is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	healthServer, err := common.NewHealthServer(8080, 8089, logger)
	if err != nil {
		logger.Error("Failed to create health server: %v", err)
		os.Exit(1)
	}
	go func() {
		if err := healthServer.Start(ctx); err != nil {
			logger.Error("Health server failed: %v", err)
			cancel()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received signal %v, cancelling in-flight walks...", sig)
		cancel()
	}()

	firestoreClient, err := common.NewFirestoreClient(ctx, config.FirestoreProject, config.FirestoreDatabase)
	if err != nil {
		logger.Error("Failed to create Firestore client: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err := firestoreClient.Close(); err != nil {
			logger.Error("Failed to close Firestore client: %v", err)
		}
	}()

	store := common.NewFirestoreFollowStore(firestoreClient, logger)
	bskyClient := followed_users_backfill.NewBskyClient(&http.Client{Timeout: 10 * time.Second})
	service := followed_users_backfill.NewService(bskyClient, store, store, logger, followed_users_backfill.ServiceConfig{
		TTL:              time.Duration(config.FollowsCacheTTLSec) * time.Second,
		MaxPendingAdds:   500, // matches api's followed_users_cache.MAX_PENDING_ADDS — keep in step
		MaxFollowedUsers: *maxFollowedUsers,
		RetentionDays:    *retentionDays,
		PerUserTimeout:   time.Duration(*perUserTimeoutSec) * time.Second,
		Concurrency:      *concurrency,
	})

	healthServer.SetHealthy(true, "Backfilling followed-users cache")

	runStart := time.Now()
	logger.Metric("followed_users_backfill.run_attempted_count", 1)
	processed, refreshed, skipped, failed, err := service.Run(ctx)
	if err != nil {
		logger.Error("Backfill run failed: %v", err)
		logger.Metric("followed_users_backfill.run_error_count", 1)
		os.Exit(1)
	}

	logger.Metric("followed_users_backfill.run_duration_ms", float64(time.Since(runStart).Milliseconds()))
	logger.Metric("followed_users_backfill.run_success_count", 1)
	logger.Info("Backfill complete: processed=%d refreshed=%d skipped=%d failed=%d", processed, refreshed, skipped, failed)
}
