package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// TODO: Move to multithreaded implementation

func main() {
	// Parse command line flags
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no writes to Elasticsearch)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	source := flag.String("source", "local", "Source of SQLite files: 'local' or 's3'")
	mode := flag.String("mode", "once", "Ingestion mode: 'once' or 'spool'")
	flag.Parse()

	// Load configuration
	config := LoadConfig()
	logger := NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - BlueSky Ingest Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no writes to Elasticsearch")
	}

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, finishing current batch...")
		cancel()
	}()

	logger.Info("Starting SQLite ingestion (source: %s, mode: %s)", *source, *mode)
	runIngestion(ctx, config, logger, *source, *mode, *dryRun, *skipTLSVerify)
}

func runIngestion(ctx context.Context, config *Config, logger *IngestLogger, source, mode string, dryRun, skipTLSVerify bool) {
	// Validate source parameter
	if source != "local" && source != "s3" {
		logger.Error("Invalid source: %s (must be 'local' or 's3')", source)
		os.Exit(1)
	}

	// Validate mode parameter
	if mode != "once" && mode != "spool" {
		logger.Error("Invalid mode: %s (must be 'once' or 'spool')", mode)
		os.Exit(1)
	}

	// Validate Elasticsearch configuration
	if config.ElasticsearchURL == "" {
		logger.Error("ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	if !dryRun && config.ElasticsearchAPIKey == "" {
		logger.Error("ELASTICSEARCH_API_KEY environment variable is required")
		os.Exit(1)
	}

	// Validate source-specific configuration
	if source == "local" {
		if config.LocalSQLiteDBPath == "" {
			logger.Error("LOCAL_SQLITE_DB_PATH environment variable is required for local source")
			os.Exit(1)
		}
	} else if source == "s3" {
		if config.S3SQLiteDBBucket == "" {
			logger.Error("S3_SQLITE_DB_BUCKET environment variable is required for s3 source")
			os.Exit(1)
		}
		if config.S3SQLiteDBPrefix == "" {
			logger.Error("S3_SQLITE_DB_PREFIX environment variable is required for s3 source")
			os.Exit(1)
		}
	}

	// Initialize state manager
	stateManager, err := NewStateManager(config.SpoolStateFile, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager: %v", err)
		os.Exit(1)
	}

	// Initialize Elasticsearch client
	esConfig := ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify,
	}

	esClient, err := NewElasticsearchClient(esConfig, logger)
	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	}

	// Initialize spooler
	var spooler Spooler
	interval := time.Duration(config.SpoolIntervalSec) * time.Second

	if source == "local" {
		spooler = NewLocalSpooler(config.LocalSQLiteDBPath, mode, interval, stateManager, logger)
	} else {
		spooler, err = NewS3Spooler(config.S3SQLiteDBBucket, config.S3SQLiteDBPrefix, config.AWSRegion, mode, interval, stateManager, logger)
		if err != nil {
			logger.Error("Failed to create S3 spooler: %v", err)
			os.Exit(1)
		}
	}

	// Start spooler
	if err := spooler.Start(ctx); err != nil {
		logger.Error("Failed to start spooler: %v", err)
		os.Exit(1)
	}

	// Process rows from spooler
	rowChan := spooler.GetRowChannel()
	var batch []ElasticsearchDoc
	var tombstoneBatch []TombstoneDoc
	var deleteBatch []string
	const batchSize = 100
	processedCount := 0
	deletedCount := 0
	skippedCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown signal received, stopping ingestion")
			goto cleanup
		case row, ok := <-rowChan:
			if !ok {
				logger.Info("Spooler channel closed, finishing remaining batch")
				goto cleanup
			}

			if row.AtURI == "" {
				logger.Error("Skipping row with empty at_uri from file %s (did: %s)", row.SourceFilename, row.DID)
				skippedCount++
				continue
			}

			msg := NewMegaStreamMessage(row.AtURI, row.DID, row.RawPost, row.Inferences, logger)

			if msg.IsDelete() {
				tombstone := CreateTombstoneDoc(msg)
				tombstoneBatch = append(tombstoneBatch, tombstone)
				deleteBatch = append(deleteBatch, msg.GetAtURI())

				if len(tombstoneBatch) >= batchSize {
					if err := bulkIndexTombstones(ctx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk index tombstones: %v", err)
					} else {
						if dryRun {
							logger.Info("Dry-run: Would index %d tombstones", len(tombstoneBatch))
						} else {
							logger.Info("Indexed %d tombstones", len(tombstoneBatch))
						}
					}

					if err := bulkDelete(ctx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk delete posts: %v", err)
					} else {
						deletedCount += len(deleteBatch)
						if dryRun {
							logger.Info("Dry-run: Would delete batch: %d posts (total deleted: %d)", len(deleteBatch), deletedCount)
						} else {
							logger.Info("Deleted batch: %d posts (total deleted: %d)", len(deleteBatch), deletedCount)
						}
					}

					tombstoneBatch = tombstoneBatch[:0]
					deleteBatch = deleteBatch[:0]
				}
				continue
			}

			doc := CreateElasticsearchDoc(msg)
			batch = append(batch, doc)

			if len(batch) >= batchSize {
				if err := bulkIndex(ctx, esClient, "posts", batch, dryRun, logger); err != nil {
					logger.Error("Failed to bulk index batch: %v", err)
				} else {
					processedCount += len(batch)
					if dryRun {
						logger.Info("Dry-run: Would index batch: %d documents (total: %d, deleted: %d, skipped: %d)", len(batch), processedCount, deletedCount, skippedCount)
					} else {
						logger.Info("Indexed batch: %d documents (total: %d, deleted: %d, skipped: %d)", len(batch), processedCount, deletedCount, skippedCount)
					}
				}
				batch = batch[:0]
			}
		}
	}

cleanup:
	// Index remaining documents in batch
	if len(batch) > 0 {
		if err := bulkIndex(ctx, esClient, "posts", batch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk index final batch: %v", err)
		} else {
			processedCount += len(batch)
			if dryRun {
				logger.Info("Dry-run: Would index final batch: %d documents", len(batch))
			} else {
				logger.Info("Indexed final batch: %d documents", len(batch))
			}
		}
	}

	// Index remaining tombstones and delete posts
	if len(tombstoneBatch) > 0 {
		if err := bulkIndexTombstones(ctx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk index final tombstone batch: %v", err)
		} else {
			if dryRun {
				logger.Info("Dry-run: Would index final batch: %d tombstones", len(tombstoneBatch))
			} else {
				logger.Info("Indexed final batch: %d tombstones", len(tombstoneBatch))
			}
		}

		if err := bulkDelete(ctx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk delete final batch: %v", err)
		} else {
			deletedCount += len(deleteBatch)
			if dryRun {
				logger.Info("Dry-run: Would delete final batch: %d posts", len(deleteBatch))
			} else {
				logger.Info("Deleted final batch: %d posts", len(deleteBatch))
			}
		}
	}

	logger.Info("Spooler ingestion complete. Processed: %d, Deleted: %d, Skipped: %d", processedCount, deletedCount, skippedCount)
}
