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
	batch      []common.LikeDoc
	timeUs     int64
	batchCount int
	skipCount  int
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

	// Start health check server
	healthServer, err := common.NewHealthServer(8080, 8089, logger)
	if err != nil {
		logger.Error("Failed to create health check server: %v", err)
		os.Exit(1)
	}
	go healthServer.Start(ctx)

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, finishing current batch...")
		cancel()
	}()

	logger.Info("Starting Jetstream likes ingestion")
	runIngestion(ctx, config, logger, healthServer, *dryRun, *skipTLSVerify, *noRewind)
}

func runIngestion(ctx context.Context, config *common.Config, logger *common.IngestLogger, healthServer *common.HealthServer, dryRun, skipTLSVerify, noRewind bool) {
	stateManager, err := common.NewStateManager(config.JetstreamStateFile, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager: %v", err)
		os.Exit(1)
	}

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
						}
					}
					cursorMu.Unlock()
				}
			}
		}()
	}

	// Start worker pool for parallel Elasticsearch writes
	const numWorkers = 3
	workersDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go esWorker(ctx, i, batchChan, esClient, &cursorMu, &pendingCursor, &hasPendingUpdate, dryRun, logger, &wg)
		}
		wg.Wait()
		close(workersDone)
	}()

	var batch []common.LikeDoc
	var lastTimeUs int64
	const batchSize = 100
	processedCount := 0
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

			// Only process like events
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
					batch:      batch,
					timeUs:     lastTimeUs,
					batchCount: len(batch),
					skipCount:  skippedCount,
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
	// Send final batch to workers
	if len(batch) > 0 {
		job := batchJob{
			batch:      batch,
			timeUs:     lastTimeUs,
			batchCount: len(batch),
			skipCount:  skippedCount,
		}

		select {
		case batchChan <- job:
			processedCount += len(batch)
		case <-time.After(5 * time.Second):
			logger.Error("Timeout sending final batch to workers")
		}
	}

	// Close batch channel to signal workers to finish
	close(batchChan)

	// Wait for all workers to complete
	<-workersDone

	logger.Info("Jetstream ingestion complete. Processed: %d, Skipped: %d", processedCount, skippedCount)
}

// esWorker processes batches of documents and writes them to Elasticsearch
func esWorker(ctx context.Context, id int, batchChan <-chan batchJob, esClient *elasticsearch.Client, cursorMu *sync.Mutex, pendingCursor *int64, hasPendingUpdate *bool, dryRun bool, logger *common.IngestLogger, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range batchChan {
		if err := common.BulkIndexLikes(ctx, esClient, "likes", job.batch, dryRun, logger); err != nil {
			logger.Error("Worker %d: Failed to bulk index likes: %v", id, err)
		} else {
			// Calculate freshness (lag in seconds)
			freshnessSeconds := calculateFreshness(job.timeUs)

			if dryRun {
				logger.Info("Worker %d: Dry-run: Would index batch: %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
			} else {
				logger.Info("Worker %d: Indexed batch: %d likes (skipped: %d, freshness: %ds)", id, job.batchCount, job.skipCount, freshnessSeconds)
			}

			// Record cursor for throttled update (written every 10 seconds by separate goroutine)
			if !dryRun {
				cursorMu.Lock()
				if job.timeUs > *pendingCursor {
					*pendingCursor = job.timeUs
					*hasPendingUpdate = true
				}
				cursorMu.Unlock()
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
