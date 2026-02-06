package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/jetstream_ingest"
)

type batchJob struct {
	batch          []common.LikeDoc
	tombstoneBatch []common.LikeTombstoneDoc
	deleteBatch    []common.DeleteDoc
	timeUs         int64
	batchCount     int
	tombstoneCount int
	skipCount      int
}

func main() {
	// Parse command line flags
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no writes to Elasticsearch)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	noRewind := flag.Bool("no-rewind", false, "Do not rewind to last processed timestamp on startup (drops intervening data)")
	maxRewindMinutes := flag.Int("max-rewind", 0, "Maximum number of minutes to rewind cursor on startup (0 = unlimited)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)
	logger.SetDebugEnabled(*debug)
	otelCollector, err := common.NewOTelMetricCollector("jetstream-ingest", config.Environment, config.GCPProjectID, config.GCPRegion, config.MetricExportIntervalSec)
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

	logger.Info("Green Earth Ingex - BlueSky Jetstream Ingest Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no writes to Elasticsearch")
	}
	if *noRewind {
		logger.Info("Rewind disabled - starting from current time")
	}

	// Validate configuration
	if config.JetstreamURL == "" {
		logger.Error("GE_JETSTREAM_URL environment variable is required")
		os.Exit(1)
	}

	if config.ElasticsearchURL == "" {
		logger.Error("GE_ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	if !*dryRun && config.ElasticsearchAPIKey == "" {
		logger.Error("GE_ELASTICSEARCH_API_KEY environment variable is required")
		os.Exit(1)
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

	logger.Info("Starting Jetstream likes ingestion")
	runIngestion(ctx, config, logger, healthServer, *dryRun, *skipTLSVerify, *noRewind, *maxRewindMinutes)
}

// checkForNewerInstance checks if another instance has started after us
// Returns true if a newer instance is detected
func runIngestion(ctx context.Context, config *common.Config, logger *common.IngestLogger, healthServer *common.HealthServer, dryRun, skipTLSVerify, noRewind bool, maxRewindMinutes int) {
	stateManager, err := common.NewStateManager(config.JetstreamStateFile, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager: %v", err)
		os.Exit(1)
	}

	// Create post routing cache to reduce ES lookups for author_did
	routingCache, err := common.NewPostRoutingCache(config.PostRoutingCacheSize)
	if err != nil {
		logger.Error("Failed to create routing cache: %v", err)
		os.Exit(1)
	}
	logger.Info("Created post routing cache with capacity %d", config.PostRoutingCacheSize)

	// Write instance coordination file with current timestamp
	// This allows other instances to detect when a new instance has started
	myStartTime := time.Now().UnixMicro()
	if err := stateManager.WriteInstanceInfo(myStartTime); err != nil {
		logger.Error("Failed to write instance info: %v", err)
		os.Exit(1)
	}
	logger.Info("Wrote instance coordination file with start time: %d", myStartTime)

	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	}

	// Initialize Jetstream client
	client := jetstream_ingest.NewClient(config.JetstreamURL, logger)

	// Apply cursor if rewind is enabled and we have a saved cursor
	if !noRewind {
		if cursor := stateManager.GetCursor(); cursor != nil {
			cursorTime := cursor.LastTimeUs

			// Apply max-rewind limit if specified
			if maxRewindMinutes > 0 {
				currentTime := time.Now().UnixMicro()
				maxRewindUs := int64(maxRewindMinutes) * 60 * 1000000 // Convert minutes to microseconds
				minAllowedTime := currentTime - maxRewindUs

				if cursorTime < minAllowedTime {
					logger.Info("Cursor %d is older than max-rewind limit (%d minutes), clamping to %d", cursorTime, maxRewindMinutes, minAllowedTime)
					cursorTime = minAllowedTime
				}
			}

			client.SetCursor(cursorTime)
			logger.Info("Rewinding to last processed timestamp: %d", cursorTime)
		}
	}

	if err := client.Start(ctx); err != nil {
		logger.Error("Failed to start Jetstream client: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error("Failed to close Jetstream client: %v", err)
		}
	}()

	// Mark service as healthy once we've successfully connected and started processing
	healthServer.SetHealthy(true, "Processing Jetstream messages")

	// Process messages from Jetstream with parallel workers
	msgChan := client.GetMessageChannel()

	// Create a channel for batches to be processed by workers
	// Can queue 50k docs (50 batches of 1000)
	batchChan := make(chan batchJob, 50)

	// Track pending cursor updates to throttle state writes
	var cursorMu sync.Mutex
	var pendingCursor int64
	var hasPendingUpdate bool
	var pendingBatchCount int
	var pendingSkipCount int

	// Start throttled state writer (writes at most once every 10 seconds)
	if !dryRun {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					// Flush any pending update before exiting
					cursorMu.Lock()
					if hasPendingUpdate {
						if err := stateManager.UpdateCursor(pendingCursor); err != nil {
							logger.Error("Failed to flush final cursor update: %v", err)
						}
					}
					cursorMu.Unlock()
					return
				case <-ticker.C:
					cursorMu.Lock()
					if hasPendingUpdate {
						if err := stateManager.UpdateCursor(pendingCursor); err != nil {
							logger.Error("Failed to update cursor: %v", err)
						} else {
							hasPendingUpdate = false
							// Log summary of batches processed since last log
							if pendingBatchCount > 0 {
								freshnessSeconds := common.CalculateFreshness(pendingCursor)
								logger.Debug("Indexed %d likes (skipped: %d, freshness: %ds)", pendingBatchCount, pendingSkipCount, freshnessSeconds)
								pendingBatchCount = 0
								pendingSkipCount = 0
							}
						}
					}
					cursorMu.Unlock()
				}
			}
		}()
	}

	// Start worker pool for parallel Elasticsearch writes
	const numWorkers = 10
	workersDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go esWorker(ctx, i, batchChan, esClient, routingCache, &cursorMu, &pendingCursor, &hasPendingUpdate, &pendingBatchCount, &pendingSkipCount, dryRun, logger, &wg)
		}
		wg.Wait()
		close(workersDone)
	}()

	var batch []common.LikeDoc
	var deleteMessages []common.JetstreamMessage
	var lastTimeUs int64
	const batchSize = 100
	processedCount := 0
	deletedCount := 0
	skippedCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown signal received, stopping ingestion")
			goto cleanup
		case rawMsg, ok := <-msgChan:
			if !ok {
				logger.Info("Jetstream channel closed, finishing remaining batch")
				goto cleanup
			}

			msg := common.NewJetstreamMessage(rawMsg, logger)

			// Handle like deletions
			if msg.IsLikeDelete() {
				if msg.GetAtURI() == "" {
					logger.Error("Skipping like deletion with empty at_uri (author_did: %s)", msg.GetAuthorDID())
					skippedCount++
					continue
				}

				// Store delete message for batch processing
				deleteMessages = append(deleteMessages, msg)

				// Track the latest timestamp
				if msg.GetTimeUs() > lastTimeUs {
					lastTimeUs = msg.GetTimeUs()
				}

				// Process batch when full
				if len(deleteMessages) >= batchSize {
					// Fetch existing like documents from Elasticsearch
					likeIDs := make([]common.LikeIdentifier, len(deleteMessages))
					for i, delMsg := range deleteMessages {
						likeIDs[i] = common.LikeIdentifier{
							AtURI:     delMsg.GetAtURI(),
							AuthorDID: delMsg.GetAuthorDID(),
						}
					}

					likeDocs, err := common.BulkGetLikes(ctx, esClient, "likes", likeIDs, logger)
					if err != nil {
						logger.Error("Failed to fetch like documents for deletion: %v", err)
						// Continue processing - we'll skip tombstone creation for missing docs
					}

					// Build tombstone and delete batches
					var tombstoneBatch []common.LikeTombstoneDoc
					var deleteBatch []common.DeleteDoc

					for _, delMsg := range deleteMessages {
						atURI := delMsg.GetAtURI()
						authorDID := delMsg.GetAuthorDID()

						// Check if we found the like document
						if likeDoc, found := likeDocs[atURI]; found {
							// Create tombstone with subject_uri from ES
							tombstone := common.CreateLikeTombstoneDoc(delMsg, likeDoc.SubjectURI)
							tombstoneBatch = append(tombstoneBatch, tombstone)
						} else {
							// This isn't an error since we won't always have the original like document
							logger.Debug("Like document not found for deletion, skipping tombstone: at_uri=%s", atURI)
						}

						// Always add to delete batch (idempotent operation)
						deleteBatch = append(deleteBatch, common.DeleteDoc{
							DocID:     atURI,
							AuthorDID: authorDID,
						})
					}

					// Send batch to workers
					job := batchJob{
						batch:          make([]common.LikeDoc, 0),
						tombstoneBatch: tombstoneBatch,
						deleteBatch:    deleteBatch,
						timeUs:         lastTimeUs,
						batchCount:     0,
						tombstoneCount: len(tombstoneBatch),
						skipCount:      skippedCount,
					}

					select {
					case batchChan <- job:
						deletedCount += len(deleteBatch)
					case <-ctx.Done():
						goto cleanup
					}

					// Reset delete messages batch
					deleteMessages = make([]common.JetstreamMessage, 0, batchSize)
				}
			} else if msg.IsLike() {

				if msg.GetAtURI() == "" {
					logger.Error("Skipping like with empty at_uri (author_did: %s)", msg.GetAuthorDID())
					skippedCount++
					continue
				}

				if msg.GetSubjectURI() == "" {
					logger.Error("Skipping like with empty subject_uri (at_uri: %s, author_did: %s)", msg.GetAtURI(), msg.GetAuthorDID())
					skippedCount++
					continue
				}

				doc := common.CreateLikeDoc(msg)
				batch = append(batch, doc)

				// Track the latest timestamp
				if msg.GetTimeUs() > lastTimeUs {
					lastTimeUs = msg.GetTimeUs()
				}

				if len(batch) >= batchSize {
					// Send batch to workers for processing
					job := batchJob{
						batch:          batch,
						tombstoneBatch: make([]common.LikeTombstoneDoc, 0),
						deleteBatch:    make([]common.DeleteDoc, 0),
						timeUs:         lastTimeUs,
						batchCount:     len(batch),
						tombstoneCount: 0,
						skipCount:      skippedCount,
					}

					select {
					case batchChan <- job:
						processedCount += len(batch)

						// Check if a newer instance has started (every 10 batches to avoid excessive GCS reads)
						if processedCount%1000 == 0 {
							if stateManager.CheckForNewerInstance(myStartTime) {
								logger.Info("Newer instance detected, exiting")
								goto cleanup
							}
						}
					case <-ctx.Done():
						goto cleanup
					}

					// Create new batch slice
					batch = make([]common.LikeDoc, 0, batchSize)
				}
			}
		}
	}

cleanup:
	// Send final like batch to workers
	if len(batch) > 0 {
		job := batchJob{
			batch:          batch,
			tombstoneBatch: make([]common.LikeTombstoneDoc, 0),
			deleteBatch:    make([]common.DeleteDoc, 0),
			timeUs:         lastTimeUs,
			batchCount:     len(batch),
			tombstoneCount: 0,
			skipCount:      skippedCount,
		}

		select {
		case batchChan <- job:
			processedCount += len(batch)
		case <-time.After(5 * time.Second):
			logger.Error("Timeout sending final like batch to workers")
		}
	}

	// Send final delete batch to workers
	if len(deleteMessages) > 0 {
		// Fetch existing like documents from Elasticsearch
		likeIDs := make([]common.LikeIdentifier, len(deleteMessages))
		for i, delMsg := range deleteMessages {
			likeIDs[i] = common.LikeIdentifier{
				AtURI:     delMsg.GetAtURI(),
				AuthorDID: delMsg.GetAuthorDID(),
			}
		}

		likeDocs, err := common.BulkGetLikes(ctx, esClient, "likes", likeIDs, logger)
		if err != nil {
			logger.Error("Failed to fetch like documents for final deletion batch: %v", err)
		}

		// Build tombstone and delete batches
		var tombstoneBatch []common.LikeTombstoneDoc
		var deleteBatch []common.DeleteDoc

		for _, delMsg := range deleteMessages {
			atURI := delMsg.GetAtURI()
			authorDID := delMsg.GetAuthorDID()

			if likeDoc, found := likeDocs[atURI]; found {
				tombstone := common.CreateLikeTombstoneDoc(delMsg, likeDoc.SubjectURI)
				tombstoneBatch = append(tombstoneBatch, tombstone)
			} else {
				logger.Debug("Like document not found for final deletion, skipping tombstone: at_uri=%s", atURI)
			}

			deleteBatch = append(deleteBatch, common.DeleteDoc{
				DocID:     atURI,
				AuthorDID: authorDID,
			})
		}

		job := batchJob{
			batch:          make([]common.LikeDoc, 0),
			tombstoneBatch: tombstoneBatch,
			deleteBatch:    deleteBatch,
			timeUs:         lastTimeUs,
			batchCount:     0,
			tombstoneCount: len(tombstoneBatch),
			skipCount:      skippedCount,
		}

		select {
		case batchChan <- job:
			deletedCount += len(deleteBatch)
		case <-time.After(5 * time.Second):
			logger.Error("Timeout sending final delete batch to workers")
		}
	}

	// Close batch channel to signal workers to finish
	close(batchChan)

	// Wait for all workers to complete
	<-workersDone

	logger.Info("Jetstream ingestion complete. Processed: %d, Deleted: %d, Skipped: %d", processedCount, deletedCount, skippedCount)
}

// esWorker processes batches of documents and writes them to Elasticsearch
func esWorker(ctx context.Context, id int, batchChan <-chan batchJob, esClient *elasticsearch.Client, routingCache *common.PostRoutingCache, cursorMu *sync.Mutex, pendingCursor *int64, hasPendingUpdate *bool, pendingBatchCount *int, pendingSkipCount *int, dryRun bool, logger *common.IngestLogger, wg *sync.WaitGroup) {
	defer wg.Done()

	batchCounter := 0
	for job := range batchChan {
		batchCounter++
		// Calculate freshness once at start
		freshnessSeconds := common.CalculateFreshness(job.timeUs)
		logger.Metric("jetstream.freshness_sec", float64(freshnessSeconds))
		success := true

		// Handle tombstone and deletion batch
		if len(job.tombstoneBatch) > 0 {
			// Index tombstones FIRST (critical for data preservation)
			if err := common.BulkIndexLikeTombstones(ctx, esClient, "like_tombstones", job.tombstoneBatch, dryRun, logger); err != nil {
				logger.Error("Worker %d: Failed to bulk index like tombstones: %v", id, err)
				success = false
			} else {
				if dryRun {
					logger.Debug("Worker %d: Dry-run: Would index %d like tombstones", id, job.tombstoneCount)
				} else {
					logger.Debug("Worker %d: Indexed %d like tombstones", id, job.tombstoneCount)
				}

				// Only delete if tombstone indexing succeeded
				if len(job.deleteBatch) > 0 {
					if err := common.BulkDelete(ctx, esClient, "likes", job.deleteBatch, dryRun, logger); err != nil {
						logger.Error("Worker %d: Failed to bulk delete likes: %v", id, err)
						success = false
					} else {
						if dryRun {
							logger.Debug("Worker %d: Dry-run: Would delete %d likes (freshness: %ds)", id, len(job.deleteBatch), freshnessSeconds)
						} else {
							logger.Debug("Worker %d: Deleted %d likes (freshness: %ds)", id, len(job.deleteBatch), freshnessSeconds)
						}

						// Decrement like counts on posts
						updates := make([]common.LikeCountUpdate, len(job.tombstoneBatch))
						for i, tombstone := range job.tombstoneBatch {
							updates[i] = common.LikeCountUpdate{
								SubjectURI: tombstone.SubjectURI,
								Increment:  -1,
							}
						}

						if err := common.BulkUpdatePostLikeCounts(ctx, esClient, "posts", updates, routingCache, dryRun, logger); err != nil {
							logger.Error("Worker %d: Failed to decrement post like counts: %v", id, err)
							// Don't set success=false - this is a secondary operation
						}
					}
				}
			}
		}

		// Handle like creation batch
		if len(job.batch) > 0 {
			if err := common.BulkIndexLikes(ctx, esClient, "likes", job.batch, dryRun, logger); err != nil {
				logger.Error("Worker %d: Failed to bulk index likes: %v", id, err)
				success = false
			} else {
				if dryRun {
					logger.Debug("Worker %d: Dry-run: Would index %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
				} else {
					logger.Debug("Worker %d: Indexed %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
				}

				// Update like counts on posts
				updates := make([]common.LikeCountUpdate, len(job.batch))
				for i, like := range job.batch {
					updates[i] = common.LikeCountUpdate{
						SubjectURI: like.SubjectURI,
						Increment:  1,
					}
				}

				if err := common.BulkUpdatePostLikeCounts(ctx, esClient, "posts", updates, routingCache, dryRun, logger); err != nil {
					logger.Error("Worker %d: Failed to update post like counts: %v", id, err)
					// Don't set success=false - this is a secondary operation
				}
			}
		}

		// Log info every 100 batches
		if batchCounter%100 == 0 {
			logger.Info("Worker %d: Processed %d batches (~%d documents)", id, batchCounter, batchCounter*100)
			batchCounter = 0
		}

		// Save cursor after successful batch operations
		if success && !dryRun {
			// Record cursor and batch stats for throttled logging (logged every 10 seconds by state writer goroutine)
			// This is necessary to avoid a GSE ratelimit on state file writes
			cursorMu.Lock()
			if job.timeUs > *pendingCursor {
				*pendingCursor = job.timeUs
				*hasPendingUpdate = true
			}
			*pendingBatchCount += job.batchCount
			*pendingSkipCount += job.skipCount
			cursorMu.Unlock()
		}
	}
}

