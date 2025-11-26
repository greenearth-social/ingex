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
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - BlueSky Jetstream Ingest Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no writes to Elasticsearch")
	}
	if *noRewind {
		logger.Info("Rewind disabled - starting from current time")
	}

	// Validate configuration
	if config.JetstreamURL == "" {
		logger.Error("JETSTREAM_URL environment variable is required")
		os.Exit(1)
	}

	if config.ElasticsearchURL == "" {
		logger.Error("ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	if !*dryRun && config.ElasticsearchAPIKey == "" {
		logger.Error("ELASTICSEARCH_API_KEY environment variable is required")
		os.Exit(1)
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

	logger.Info("Starting Jetstream likes ingestion")
	runIngestion(ctx, config, logger, *dryRun, *skipTLSVerify, *noRewind)
}

func runIngestion(ctx context.Context, config *common.Config, logger *common.IngestLogger, dryRun, skipTLSVerify, noRewind bool) {
	stateManager, err := common.NewStateManager(config.JetstreamStateFile, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager: %v", err)
		os.Exit(1)
	}

	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify,
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
			client.SetCursor(cursor.LastTimeUs)
			logger.Info("Rewinding to last processed timestamp: %d", cursor.LastTimeUs)
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

	// Process messages from Jetstream with parallel workers
	msgChan := client.GetMessageChannel()

	// Create a channel for batches to be processed by workers
	// Can queue 50k docs (50 batches of 1000)
	batchChan := make(chan batchJob, 50)

	// Start worker pool for parallel Elasticsearch writes
	const numWorkers = 3
	workersDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go esWorker(ctx, i, batchChan, esClient, stateManager, dryRun, logger, &wg)
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
							logger.Error("Like document not found for deletion, skipping tombstone: at_uri=%s", atURI)
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
				continue
			}

			// Only process like creation events
			if !msg.IsLike() {
				continue
			}

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
				case <-ctx.Done():
					goto cleanup
				}

				// Create new batch slice
				batch = make([]common.LikeDoc, 0, batchSize)
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
				logger.Error("Like document not found for final deletion, skipping tombstone: at_uri=%s", atURI)
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
func esWorker(ctx context.Context, id int, batchChan <-chan batchJob, esClient *elasticsearch.Client, stateManager *common.StateManager, dryRun bool, logger *common.IngestLogger, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range batchChan {
		// Calculate freshness once at start
		freshnessSeconds := calculateFreshness(job.timeUs)
		success := true

		// Handle tombstone and deletion batch
		if len(job.tombstoneBatch) > 0 {
			// Index tombstones FIRST (critical for data preservation)
			if err := common.BulkIndexLikeTombstones(ctx, esClient, "like_tombstones", job.tombstoneBatch, dryRun, logger); err != nil {
				logger.Error("Worker %d: Failed to bulk index like tombstones: %v", id, err)
				success = false
			} else {
				if dryRun {
					logger.Info("Worker %d: Dry-run: Would index %d like tombstones", id, job.tombstoneCount)
				} else {
					logger.Info("Worker %d: Indexed %d like tombstones", id, job.tombstoneCount)
				}

				// Only delete if tombstone indexing succeeded
				if len(job.deleteBatch) > 0 {
					if err := common.BulkDelete(ctx, esClient, "likes", job.deleteBatch, dryRun, logger); err != nil {
						logger.Error("Worker %d: Failed to bulk delete likes: %v", id, err)
						success = false
					} else {
						if dryRun {
							logger.Info("Worker %d: Dry-run: Would delete %d likes (freshness: %ds)", id, len(job.deleteBatch), freshnessSeconds)
						} else {
							logger.Info("Worker %d: Deleted %d likes (freshness: %ds)", id, len(job.deleteBatch), freshnessSeconds)
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
					logger.Info("Worker %d: Dry-run: Would index %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
				} else {
					logger.Info("Worker %d: Indexed %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
				}
			}
		}

		// Save cursor after successful batch operations
		if success && !dryRun {
			if err := stateManager.UpdateCursor(job.timeUs); err != nil {
				logger.Error("Worker %d: Failed to update cursor: %v", id, err)
			}
		}
	}
}

// calculateFreshness returns the lag in seconds between the given timestamp and now
func calculateFreshness(timeUs int64) int64 {
	if timeUs == 0 {
		return 0
	}
	nowUs := time.Now().UnixMicro()
	lagUs := nowUs - timeUs
	return lagUs / 1_000_000 // Convert microseconds to seconds
}
