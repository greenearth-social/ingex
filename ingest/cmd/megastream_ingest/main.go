package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/megastream_ingest"
)

// TODO: Move to multithreaded implementation

func main() {
	// Parse command line flags
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no writes to Elasticsearch)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	source := flag.String("source", "local", "Source of SQLite files: 'local' or 's3'")
	mode := flag.String("mode", "once", "Ingestion mode: 'once' or 'spool'")
	noRewind := flag.Bool("no-rewind", false, "Do not rewind to last processed timestamp on startup (drops intervening data)")
	startupWithLastFile := flag.Bool("startup-with-last-file", false, "Process the most recent file on startup, even if before the default cursor")
	maxRewindMinutes := flag.Int("max-rewind", 0, "Maximum number of minutes to rewind cursor on startup (0 = unlimited)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)
	logger.SetDebugEnabled(*debug)

	logger.Info("Green Earth Ingex - BlueSky Ingest Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no writes to Elasticsearch")
	}
	if *noRewind {
		logger.Info("Rewind disabled - starting from current time")
	}
	if *startupWithLastFile {
		logger.Info("Startup-with-last-file enabled - will process most recent file on startup")
	}

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start health check server
	healthServer, err := common.NewHealthServer(8080, 8089, logger)
	if err != nil {
		logger.Error("Failed to create health check server: %v", err)
		os.Exit(1)
	}
	go func() {
		if err := healthServer.Start(ctx); err != nil {
			logger.Error("Health server failed: %v", err)
			cancel()
		}
	}()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, finishing current batch...")
		cancel()
	}()

	logger.Info("Starting SQLite ingestion (source: %s, mode: %s)", *source, *mode)
	if err := runIngestion(ctx, config, logger, healthServer, *source, *mode, *dryRun, *skipTLSVerify, *noRewind, *startupWithLastFile, *maxRewindMinutes); err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	}
}

func runIngestion(ctx context.Context, config *common.Config, logger *common.IngestLogger, healthServer *common.HealthServer, source, mode string, dryRun, skipTLSVerify, noRewind, startupWithLastFile bool, maxRewindMinutes int) error {
	// Validate source parameter
	if source != "local" && source != "s3" {
		return fmt.Errorf("invalid source: %s (must be 'local' or 's3')", source)
	}

	// Validate mode parameter
	if mode != "once" && mode != "spool" {
		return fmt.Errorf("invalid mode: %s (must be 'once' or 'spool')", mode)
	}

	// Validate Elasticsearch configuration
	if config.ElasticsearchURL == "" {
		return fmt.Errorf("GE_ELASTICSEARCH_URL environment variable is required")
	}

	if !dryRun && config.ElasticsearchAPIKey == "" {
		return fmt.Errorf("GE_ELASTICSEARCH_API_KEY environment variable is required")
	}

	// Validate source-specific configuration
	switch source {
	case "local":
		if config.LocalSQLiteDBPath == "" {
			return fmt.Errorf("GE_LOCAL_SQLITE_DB_PATH environment variable is required for local source")
		}
	case "s3":
		if config.S3SQLiteDBBucket == "" {
			return fmt.Errorf("GE_AWS_S3_BUCKET environment variable is required for s3 source")
		}
		if config.S3SQLiteDBPrefix == "" {
			return fmt.Errorf("GE_AWS_S3_PREFIX environment variable is required for s3 source")
		}
	}

	// Initialize state manager
	stateManager, err := common.NewStateManager(config.MegastreamStateFile, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize state manager: %w", err)
	}

	// Handle cursor initialization based on flags
	if noRewind {
		// If no-rewind is enabled, update cursor to current time (service start time)
		currentTime := time.Now().UnixMicro()
		if err := stateManager.UpdateCursor(currentTime); err != nil {
			return fmt.Errorf("failed to update cursor for no-rewind mode: %w", err)
		}
		logger.Info("No-rewind mode: set cursor to service start time: %d", currentTime)
	} else if maxRewindMinutes > 0 {
		// Apply max-rewind limit if specified
		if cursor := stateManager.GetCursor(); cursor != nil {
			currentTime := time.Now().UnixMicro()
			maxRewindUs := int64(maxRewindMinutes) * 60 * 1000000 // Convert minutes to microseconds
			minAllowedTime := currentTime - maxRewindUs

			if cursor.LastTimeUs < minAllowedTime {
				logger.Info("Cursor %d is older than max-rewind limit (%d minutes), clamping to %d", cursor.LastTimeUs, maxRewindMinutes, minAllowedTime)
				if err := stateManager.UpdateCursor(minAllowedTime); err != nil {
					return fmt.Errorf("failed to update cursor for max-rewind limit: %w", err)
				}
			}
		}
	} else if startupWithLastFile {
		// If startup-with-last-file is enabled, find and set cursor to process the most recent file
		var mostRecentFileTime int64
		var err error

		switch source {
		case "local":
			mostRecentFileTime, err = findMostRecentLocalFile(config.LocalSQLiteDBPath, logger)
		case "s3":
			mostRecentFileTime, err = findMostRecentS3File(ctx, config.S3SQLiteDBBucket, config.S3SQLiteDBPrefix, config.AWSRegion, config.AWSS3AccessKey, config.AWSS3SecretKey, logger)
		}

		if err != nil {
			return fmt.Errorf("failed to find most recent file for startup-with-last-file mode: %w", err)
		}

		if mostRecentFileTime > 0 {
			// Set cursor to just before the most recent file so it gets processed
			cursorTime := mostRecentFileTime - 1
			if err := stateManager.UpdateCursor(cursorTime); err != nil {
				return fmt.Errorf("failed to update cursor for startup-with-last-file mode: %w", err)
			}
			logger.Info("Startup-with-last-file mode: set cursor to %d to process most recent file", cursorTime)
		} else {
			logger.Info("Startup-with-last-file mode: no files found, using default cursor")
		}
	} else {
		cursor := stateManager.GetCursor()
		if cursor != nil {
			logger.Info("Rewinding to last processed timestamp: %d", cursor.LastTimeUs)
		}
	}

	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		return err
	}

	// Initialize spooler
	var spooler megastream_ingest.Spooler
	interval := time.Duration(config.SpoolIntervalSec) * time.Second

	if source == "local" {
		spooler = megastream_ingest.NewLocalSpooler(config.LocalSQLiteDBPath, mode, interval, stateManager, logger)
	} else {
		spooler, err = megastream_ingest.NewS3Spooler(config.S3SQLiteDBBucket, config.S3SQLiteDBPrefix, config.AWSRegion, config.AWSS3AccessKey, config.AWSS3SecretKey, mode, interval, stateManager, logger)
		if err != nil {
			return fmt.Errorf("failed to create S3 spooler: %w", err)
		}
	}

	// Start spooler
	if err := spooler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start spooler: %w", err)
	}

	// Mark service as healthy once we've successfully started the spooler
	healthServer.SetHealthy(true, fmt.Sprintf("Processing %s data in %s mode", source, mode))

	// Process rows from spooler
	rowChan := spooler.GetRowChannel()
	var batch []common.ElasticsearchDoc
	var msgs []common.MegaStreamMessage
	var tombstoneBatch []common.PostTombstoneDoc
	var deleteBatch []common.DeleteDoc
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

			msg := common.NewMegaStreamMessage(row.AtURI, row.DID, row.RawPost, row.Inferences, logger)

			// Skip rows with empty at_uri unless it's an account deletion event
			if row.AtURI == "" && !msg.IsAccountDeletion() {
				logger.Error("Skipping row with empty at_uri from file %s (did: %s)", row.SourceFilename, row.DID)
				skippedCount++
				continue
			}

			// Handle different event types with if-else chain
			if msg.IsAccountDeletion() {
				// Flush all pending batches before account deletion
				// This prevents post creation/deletion events from being processed
				// after the account deletion (which would be out of order)

				// Flush post creation batch
				if len(msgs) > 0 {
					batchCtx, cancelBatchCtx := context.WithTimeout(context.Background(), 30*time.Second)

					// Extract at_uris for like count query
					atURIs := make([]string, len(msgs))
					for i, m := range msgs {
						atURIs[i] = m.GetAtURI()
					}

					// Query like counts
					likeCounts, err := common.BulkCountLikesBySubjectURIs(batchCtx, esClient, "likes", atURIs, logger)
					if err != nil {
						logger.Error("Failed to query like counts before account deletion: %v (proceeding with zero counts)", err)
						likeCounts = make(map[string]int)
					}

					// Create docs with like counts
					batch = make([]common.ElasticsearchDoc, 0, len(msgs))
					for _, m := range msgs {
						likeCount := likeCounts[m.GetAtURI()]
						doc := common.CreateElasticsearchDoc(m, likeCount)
						batch = append(batch, doc)
					}

					// Index batch
					if err := common.BulkIndex(batchCtx, esClient, "posts", batch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk index batch before account deletion: %v", err)
					} else {
						processedCount += len(batch)
						if dryRun {
							logger.Info("Dry-run: Would index batch before account deletion: %d documents", len(batch))
						} else {
							logger.Info("Indexed batch before account deletion: %d documents", len(batch))
						}
					}
					batch = batch[:0]
					msgs = msgs[:0]
					cancelBatchCtx()
				}

				// Flush post deletion batch (tombstones + deletes)
				if len(tombstoneBatch) > 0 {
					batchCtx, cancelBatchCtx := context.WithTimeout(context.Background(), 30*time.Second)
					if err := common.BulkIndexPostTombstones(batchCtx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk index tombstones before account deletion: %v", err)
					} else {
						if dryRun {
							logger.Debug("Dry-run: Would index tombstones before account deletion: %d", len(tombstoneBatch))
						} else {
							logger.Debug("Indexed tombstones before account deletion: %d", len(tombstoneBatch))
						}
					}
					if err := common.BulkDelete(batchCtx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk delete posts before account deletion: %v", err)
					} else {
						deletedCount += len(deleteBatch)
						if dryRun {
							logger.Debug("Dry-run: Would delete posts before account deletion: %d", len(deleteBatch))
						} else {
							logger.Debug("Deleted posts before account deletion: %d", len(deleteBatch))
						}
					}
					tombstoneBatch = tombstoneBatch[:0]
					deleteBatch = deleteBatch[:0]
					cancelBatchCtx()
				}

				// Now process account deletion
				if err := handleAccountDeletion(ctx, msg, esClient, dryRun, logger, &deletedCount); err != nil {
					logger.Error("Failed to handle account deletion for DID %s: %v", msg.GetAuthorDID(), err)
				}
			} else if msg.IsDelete() {
				// Post deletion - add to batch
				tombstone := common.CreatePostTombstoneDoc(msg)
				tombstoneBatch = append(tombstoneBatch, tombstone)
				deleteBatch = append(deleteBatch, common.DeleteDoc{
					DocID:     msg.GetAtURI(),
					AuthorDID: msg.GetAuthorDID(),
				})

				if len(tombstoneBatch) >= batchSize {
					batchCtx, cancelBatchCtx := context.WithTimeout(context.Background(), 30*time.Second)
					if err := common.BulkIndexPostTombstones(batchCtx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk index tombstones: %v", err)
					} else {
						if dryRun {
							logger.Debug("Dry-run: Would index %d tombstones", len(tombstoneBatch))
						} else {
							logger.Debug("Indexed %d tombstones", len(tombstoneBatch))
						}
					}

					if err := common.BulkDelete(batchCtx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk delete posts: %v", err)
					} else {
						deletedCount += len(deleteBatch)
						if dryRun {
							logger.Debug("Dry-run: Would delete batch: %d posts (total deleted: %d)", len(deleteBatch), deletedCount)
						} else {
							logger.Debug("Deleted batch: %d posts (total deleted: %d)", len(deleteBatch), deletedCount)
						}
					}

					tombstoneBatch = tombstoneBatch[:0]
					deleteBatch = deleteBatch[:0]
					cancelBatchCtx()
				}
			} else {
				// Post creation - accumulate messages first
				msgs = append(msgs, msg)

				if len(msgs) >= batchSize {
					// Extract at_uris for like count query
					atURIs := make([]string, len(msgs))
					for i, m := range msgs {
						atURIs[i] = m.GetAtURI()
					}

					// Query like counts (with timeout)
					batchCtx, cancelBatchCtx := context.WithTimeout(context.Background(), 30*time.Second)
					likeCounts, err := common.BulkCountLikesBySubjectURIs(batchCtx, esClient, "likes", atURIs, logger)
					if err != nil {
						logger.Error("Failed to query like counts: %v (proceeding with zero counts)", err)
						likeCounts = make(map[string]int)
					}

					// Create docs with like counts
					batch = make([]common.ElasticsearchDoc, 0, len(msgs))
					for _, m := range msgs {
						likeCount := likeCounts[m.GetAtURI()]
						doc := common.CreateElasticsearchDoc(m, likeCount)
						batch = append(batch, doc)
					}

					// Index batch
					if err := common.BulkIndex(batchCtx, esClient, "posts", batch, dryRun, logger); err != nil {
						logger.Error("Failed to bulk index batch: %v", err)
					} else {
						processedCount += len(batch)
						if dryRun {
							logger.Debug("Dry-run: Would index batch: %d documents (total: %d, deleted: %d, skipped: %d)", len(batch), processedCount, deletedCount, skippedCount)
						} else {
							logger.Debug("Indexed batch: %d documents (total: %d, deleted: %d, skipped: %d)", len(batch), processedCount, deletedCount, skippedCount)
						}
						// Log info every 100 batches (~10k documents)
						if (processedCount / len(batch) % 100) == 0 {
							logger.Info("Progress: %d documents processed (deleted: %d, skipped: %d)", processedCount, deletedCount, skippedCount)
						}
					}
					batch = batch[:0]
					msgs = msgs[:0]
					cancelBatchCtx()
				}
			}
		}
	}

cleanup:
	// Create a separate context for cleanup operations with a 30-second timeout
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	// Index remaining documents in batch
	if len(msgs) > 0 {
		// Extract at_uris
		atURIs := make([]string, len(msgs))
		for i, m := range msgs {
			atURIs[i] = m.GetAtURI()
		}

		// Query like counts
		likeCounts, err := common.BulkCountLikesBySubjectURIs(cleanupCtx, esClient, "likes", atURIs, logger)
		if err != nil {
			logger.Error("Failed to query like counts for final batch: %v (proceeding with zero counts)", err)
			likeCounts = make(map[string]int)
		}

		// Create docs with like counts
		batch = make([]common.ElasticsearchDoc, 0, len(msgs))
		for _, m := range msgs {
			likeCount := likeCounts[m.GetAtURI()]
			doc := common.CreateElasticsearchDoc(m, likeCount)
			batch = append(batch, doc)
		}

		// Index batch
		if err := common.BulkIndex(cleanupCtx, esClient, "posts", batch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk index final batch: %v", err)
		} else {
			processedCount += len(batch)
			if dryRun {
				logger.Debug("Dry-run: Would index final batch: %d documents", len(batch))
			} else {
				logger.Debug("Indexed final batch: %d documents", len(batch))
			}
		}
	}

	// Index remaining tombstones and delete posts
	if len(tombstoneBatch) > 0 {
		if err := common.BulkIndexPostTombstones(cleanupCtx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk index final tombstone batch: %v", err)
		} else {
			if dryRun {
				logger.Debug("Dry-run: Would index final batch: %d tombstones", len(tombstoneBatch))
			} else {
				logger.Debug("Indexed final batch: %d tombstones", len(tombstoneBatch))
			}
		}

		if err := common.BulkDelete(cleanupCtx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk delete final batch: %v", err)
		} else {
			deletedCount += len(deleteBatch)
			if dryRun {
				logger.Debug("Dry-run: Would delete final batch: %d posts", len(deleteBatch))
			} else {
				logger.Debug("Deleted final batch: %d posts", len(deleteBatch))
			}
		}
	}

	logger.Info("Spooler ingestion complete. Processed: %d, Deleted: %d, Skipped: %d", processedCount, deletedCount, skippedCount)
	return nil
}

// handleAccountDeletion handles account deletion events by querying and deleting all posts and likes
func handleAccountDeletion(
	ctx context.Context,
	msg common.MegaStreamMessage,
	esClient *elasticsearch.Client,
	dryRun bool,
	logger *common.IngestLogger,
	deletedCount *int,
) error {
	authorDID := msg.GetAuthorDID()
	logger.Debug("Processing account deletion for DID: %s", authorDID)

	// Create 1-minute timeout context for queries
	queryCtx, queryCancel := context.WithTimeout(ctx, time.Minute)
	defer queryCancel()

	// Query all posts
	posts, err := common.QueryPostsByAuthorDID(queryCtx, esClient, "posts", authorDID, logger)
	if err != nil {
		return fmt.Errorf("failed to query posts for account deletion (DID: %s): %w", authorDID, err)
	}
	logger.Debug("Found %d posts for account deletion (DID: %s)", len(posts), authorDID)

	// Query all likes
	likes, err := common.QueryLikesByAuthorDID(queryCtx, esClient, "likes", authorDID, logger)
	if err != nil {
		return fmt.Errorf("failed to query likes for account deletion (DID: %s): %w", authorDID, err)
	}
	logger.Debug("Found %d likes for account deletion (DID: %s)", len(likes), authorDID)

	// Process post deletions
	if err := processAccountPostDeletions(ctx, posts, esClient, authorDID, msg.GetTimeUs(), dryRun, logger); err != nil {
		return fmt.Errorf("failed to process post deletions for account (DID: %s): %w", authorDID, err)
	}
	*deletedCount += len(posts)

	// Process like deletions
	if err := processAccountLikeDeletions(ctx, likes, esClient, authorDID, msg.GetTimeUs(), dryRun, logger); err != nil {
		return fmt.Errorf("failed to process like deletions for account (DID: %s): %w", authorDID, err)
	}
	*deletedCount += len(likes)

	logger.Debug("Completed account deletion for DID: %s (posts: %d, likes: %d)", authorDID, len(posts), len(likes))
	return nil
}

// processAccountPostDeletions processes post deletions in batches for account deletion
func processAccountPostDeletions(
	ctx context.Context,
	postAtURIs []string,
	esClient *elasticsearch.Client,
	authorDID string,
	timeUs int64,
	dryRun bool,
	logger *common.IngestLogger,
) error {
	const batchSize = 100

	now := time.Now().UTC()
	deletedAt := now
	if timeUs > 0 {
		deletedAt = time.Unix(0, timeUs*1000)
	}

	var tombstoneBatch []common.PostTombstoneDoc
	var deleteBatch []common.DeleteDoc

	for _, atURI := range postAtURIs {
		tombstoneBatch = append(tombstoneBatch, common.PostTombstoneDoc{
			AtURI:     atURI,
			AuthorDID: authorDID,
			DeletedAt: deletedAt.Format(time.RFC3339),
			IndexedAt: now.Format(time.RFC3339),
		})

		deleteBatch = append(deleteBatch, common.DeleteDoc{
			DocID:     atURI,
			AuthorDID: authorDID,
		})

		// Flush batch when full
		if len(tombstoneBatch) >= batchSize {
			if err := flushPostDeletionBatch(ctx, esClient, tombstoneBatch, deleteBatch, dryRun, logger); err != nil {
				return err
			}
			tombstoneBatch = tombstoneBatch[:0]
			deleteBatch = deleteBatch[:0]
		}
	}

	// Flush remaining
	if len(tombstoneBatch) > 0 {
		return flushPostDeletionBatch(ctx, esClient, tombstoneBatch, deleteBatch, dryRun, logger)
	}

	return nil
}

// processAccountLikeDeletions processes like deletions in batches for account deletion
func processAccountLikeDeletions(
	ctx context.Context,
	likes map[string]string,
	esClient *elasticsearch.Client,
	authorDID string,
	timeUs int64,
	dryRun bool,
	logger *common.IngestLogger,
) error {
	const batchSize = 100

	now := time.Now().UTC()
	deletedAt := now
	if timeUs > 0 {
		deletedAt = time.Unix(0, timeUs*1000)
	}

	var tombstoneBatch []common.LikeTombstoneDoc
	var deleteBatch []common.DeleteDoc

	for atURI, subjectURI := range likes {
		tombstoneBatch = append(tombstoneBatch, common.LikeTombstoneDoc{
			AtURI:      atURI,
			AuthorDID:  authorDID,
			SubjectURI: subjectURI,
			DeletedAt:  deletedAt.Format(time.RFC3339),
			IndexedAt:  now.Format(time.RFC3339),
		})

		deleteBatch = append(deleteBatch, common.DeleteDoc{
			DocID:     atURI,
			AuthorDID: authorDID,
		})

		// Flush batch when full
		if len(tombstoneBatch) >= batchSize {
			if err := flushLikeDeletionBatch(ctx, esClient, tombstoneBatch, deleteBatch, dryRun, logger); err != nil {
				return err
			}
			tombstoneBatch = tombstoneBatch[:0]
			deleteBatch = deleteBatch[:0]
		}
	}

	// Flush remaining
	if len(tombstoneBatch) > 0 {
		return flushLikeDeletionBatch(ctx, esClient, tombstoneBatch, deleteBatch, dryRun, logger)
	}

	return nil
}

// flushPostDeletionBatch indexes post tombstones and deletes posts
func flushPostDeletionBatch(
	ctx context.Context,
	esClient *elasticsearch.Client,
	tombstoneBatch []common.PostTombstoneDoc,
	deleteBatch []common.DeleteDoc,
	dryRun bool,
	logger *common.IngestLogger,
) error {
	batchCtx, cancelBatchCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBatchCtx()

	// Index tombstones first
	if err := common.BulkIndexPostTombstones(batchCtx, esClient, "post_tombstones", tombstoneBatch, dryRun, logger); err != nil {
		return fmt.Errorf("failed to bulk index post tombstones: %w", err)
	}

	// Then delete posts
	if err := common.BulkDelete(batchCtx, esClient, "posts", deleteBatch, dryRun, logger); err != nil {
		return fmt.Errorf("failed to bulk delete posts: %w", err)
	}

	return nil
}

// flushLikeDeletionBatch indexes like tombstones and deletes likes
func flushLikeDeletionBatch(
	ctx context.Context,
	esClient *elasticsearch.Client,
	tombstoneBatch []common.LikeTombstoneDoc,
	deleteBatch []common.DeleteDoc,
	dryRun bool,
	logger *common.IngestLogger,
) error {
	batchCtx, cancelBatchCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBatchCtx()

	// Index tombstones first
	if err := common.BulkIndexLikeTombstones(batchCtx, esClient, "like_tombstones", tombstoneBatch, dryRun, logger); err != nil {
		return fmt.Errorf("failed to bulk index like tombstones: %w", err)
	}

	// Then delete likes
	if err := common.BulkDelete(batchCtx, esClient, "likes", deleteBatch, dryRun, logger); err != nil {
		return fmt.Errorf("failed to bulk delete likes: %w", err)
	}

	return nil
}

// findMostRecentLocalFile finds the most recent file in the local directory
func findMostRecentLocalFile(directory string, logger *common.IngestLogger) (int64, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	var mostRecentTime int64
	var mostRecentFile string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".db.zip") {
			continue
		}

		fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(entry.Name())
		if err != nil {
			logger.Debug("Skipping file with invalid filename format: %s (%v)", entry.Name(), err)
			continue
		}

		if fileTimeUs > mostRecentTime {
			mostRecentTime = fileTimeUs
			mostRecentFile = entry.Name()
		}
	}

	if mostRecentFile != "" {
		logger.Info("Found most recent local file: %s (timestamp: %d)", mostRecentFile, mostRecentTime)
	}

	return mostRecentTime, nil
}

// findMostRecentS3File finds the most recent file in the S3 bucket
func findMostRecentS3File(ctx context.Context, bucket, prefix, region, accessKey, secretKey string, logger *common.IngestLogger) (int64, error) {
	// Create AWS S3 client
	var cfg aws.Config
	var err error

	if accessKey != "" && secretKey != "" {
		cfg, err = config.LoadDefaultConfig(
			ctx,
			config.WithRegion(region),
			config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				}, nil
			})),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
	}

	if err != nil {
		return 0, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// Start searching from 1 hour ago to avoid scanning the entire bucket
	// Files are published every ~5 minutes, so 1 hour should give us plenty of candidates
	oneHourAgo := time.Now().Add(-1 * time.Hour).UnixMicro()
	startAfterFilename := common.TimestampToMegastreamFilename(oneHourAgo)
	startAfterKey := prefix + startAfterFilename

	logger.Debug("Searching for most recent file starting from: %s", startAfterFilename)

	// List objects starting from 1 hour ago
	input := &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(prefix),
		StartAfter:   aws.String(startAfterKey),
		RequestPayer: "requester",
		MaxKeys:      aws.Int32(1000), // Limit to 1000 files (~83 hours worth at 5min intervals)
	}

	var mostRecentTime int64
	var mostRecentFile string

	// We only need to check recent files, so limit pagination
	pageCount := 0
	maxPages := 5 // At most 5 pages (5000 files = ~17 days worth)

	for pageCount < maxPages {
		result, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return 0, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		pageCount++

		for _, obj := range result.Contents {
			key := *obj.Key
			filename := filepath.Base(key)

			if !strings.HasSuffix(filename, ".db.zip") {
				continue
			}

			fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(filename)
			if err != nil {
				logger.Debug("Skipping file with invalid filename format: %s (%v)", filename, err)
				continue
			}

			if fileTimeUs > mostRecentTime {
				mostRecentTime = fileTimeUs
				mostRecentFile = filename
			}
		}

		if !*result.IsTruncated {
			break
		}

		input.ContinuationToken = result.NextContinuationToken
		input.StartAfter = nil // Only use StartAfter on first request
	}

	if mostRecentFile != "" {
		logger.Info("Found most recent S3 file: %s (timestamp: %d) after checking %d page(s)", mostRecentFile, mostRecentTime, pageCount)
	} else {
		logger.Info("No recent files found in S3 bucket (searched from %s)", startAfterFilename)
	}

	return mostRecentTime, nil
}
