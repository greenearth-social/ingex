package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/elasticsearch_expiry"
)

func main() {
	// Parse command line flags
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (show what would be deleted without actually deleting)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	retentionHours := flag.Int("retention-hours", 1440, "Number of hours to retain data (default: 1440 hours = 60 days)")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - Elasticsearch Expiry Service")
	logger.Info("Retention period: %d hours (%.1f days)", *retentionHours, float64(*retentionHours)/24.0)

	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no documents will be deleted")
	}

	// Validate configuration
	if config.ElasticsearchURL == "" {
		logger.Error("ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	if !*dryRun && config.ElasticsearchAPIKey == "" {
		logger.Error("ELASTICSEARCH_API_KEY environment variable is required (not needed in dry-run mode)")
		os.Exit(1)
	}

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start health check server
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

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received signal %v, shutting down gracefully...", sig)
		cancel()
	}()

	// Run the expiry process
	if err := runExpiry(ctx, config, logger, healthServer, *dryRun, *skipTLSVerify, *retentionHours); err != nil {
		logger.Error("Expiry process failed: %v", err)
		os.Exit(1)
	}

	logger.Info("Expiry process completed successfully")
}

func runExpiry(ctx context.Context, config *common.Config, logger *common.IngestLogger, healthServer *common.HealthServer, dryRun, skipTLSVerify bool, retentionHours int) error {
	// Default graceful timeout for delete operations during shutdown
	const graceTimeout = 30 * time.Second
	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		return fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// Calculate the cutoff date using hours
	cutoffDate := time.Now().UTC().Add(-time.Duration(retentionHours) * time.Hour)
	logger.Info("Deleting documents older than: %s", cutoffDate.Format(time.RFC3339))

	// Initialize the expiry service
	expiryConfig := elasticsearch_expiry.Config{
		CutoffDate: cutoffDate,
		DryRun:     dryRun,
	}

	expiryService := elasticsearch_expiry.NewService(esClient, expiryConfig, logger)

	// Mark service as healthy once we've successfully initialized
	healthServer.SetHealthy(true, fmt.Sprintf("Expiring documents older than %d hours (%.1f days)", retentionHours, float64(retentionHours)/24.0))

	// Define the collections to clean up
	// For now, always use the indexed_at field for expiry, since we care about how long the row has
	// been in our database, not the time the original event occurred.
	collections := []elasticsearch_expiry.Collection{
		{
			IndexAlias: "posts",
			DateField:  "indexed_at",
		},
		{
			IndexAlias: "likes",
			DateField:  "indexed_at",
		},
		{
			IndexAlias: "post_tombstones",
			DateField:  "indexed_at",
		},
		{
			IndexAlias: "like_tombstones",
			DateField:  "indexed_at",
		},
	}

	// Process each collection with graceful shutdown handling
	totalDeleted := 0
	for _, collection := range collections {
		// Check if shutdown was requested before processing each collection
		select {
		case <-ctx.Done():
			logger.Info("Shutdown requested, stopping expiry process")
			return ctx.Err()
		default:
		}

		// Create a separate context for delete operations with graceful timeout
		deleteCtx, deleteCancel := context.WithCancel(context.Background())

		// Monitor for shutdown signal and provide graceful timeout for in-flight operations
		go func() {
			<-ctx.Done()
			logger.Info("Shutdown requested, allowing %v for collection %s to complete...", graceTimeout, collection.IndexAlias)

			// Give in-flight operations time to complete gracefully
			timer := time.NewTimer(graceTimeout)
			defer timer.Stop()

			<-timer.C
			logger.Info("Grace timeout expired for collection %s, cancelling operations", collection.IndexAlias)
			deleteCancel()
		}()

		logger.Info("Processing collection: %s (date field: %s)", collection.IndexAlias, collection.DateField)

		deletedCount, err := expiryService.ExpireCollection(deleteCtx, collection)
		deleteCancel() // Clean up the context

		if err != nil {
			return fmt.Errorf("failed to expire collection %s: %w", collection.IndexAlias, err)
		}

		totalDeleted += deletedCount
		logger.Info("Processed %s: %d documents %s", collection.IndexAlias, deletedCount,
			func() string {
				if dryRun {
					return "would be deleted"
				}
				return "deleted"
			}())
	}

	action := "deleted"
	if dryRun {
		action = "would be deleted"
	}
	logger.Info("Expiry complete: %d total documents %s across all collections", totalDeleted, action)

	return nil
}
