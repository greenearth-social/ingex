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
	retentionDays := flag.Int("retention-days", 60, "Number of days to retain data (default: 60 days = ~2 months)")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - Elasticsearch Expiry Service")
	logger.Info("Retention period: %d days", *retentionDays)

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

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received signal %v, shutting down gracefully...", sig)
		cancel()
	}()

	// Run the expiry process
	if err := runExpiry(ctx, config, logger, *dryRun, *skipTLSVerify, *retentionDays); err != nil {
		logger.Error("Expiry process failed: %v", err)
		os.Exit(1)
	}

	logger.Info("Expiry process completed successfully")
}

func runExpiry(ctx context.Context, config *common.Config, logger *common.IngestLogger, dryRun, skipTLSVerify bool, retentionDays int) error {
	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		return fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// Calculate the cutoff date
	cutoffDate := time.Now().UTC().AddDate(0, 0, -retentionDays)
	logger.Info("Deleting documents older than: %s", cutoffDate.Format(time.RFC3339))

	// Initialize the expiry service
	expiryConfig := elasticsearch_expiry.Config{
		CutoffDate: cutoffDate,
		DryRun:     dryRun,
	}

	expiryService := elasticsearch_expiry.NewService(esClient, expiryConfig, logger)

	// Define the collections to clean up
	// Each collection has different date fields to check
	collections := []elasticsearch_expiry.Collection{
		{
			IndexAlias: "posts",
			DateField:  "created_at", // Use created_at as the primary date for posts
		},
		{
			IndexAlias: "likes",
			DateField:  "created_at", // Use created_at as the primary date for likes
		},
		{
			IndexAlias: "post_tombstones",
			DateField:  "deleted_at", // Use deleted_at for tombstones (when they were deleted)
		},
	}

	// Process each collection
	totalDeleted := 0
	for _, collection := range collections {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown requested, stopping expiry process")
			return ctx.Err()
		default:
		}

		logger.Info("Processing collection: %s (date field: %s)", collection.IndexAlias, collection.DateField)

		deletedCount, err := expiryService.ExpireCollection(ctx, collection)
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
