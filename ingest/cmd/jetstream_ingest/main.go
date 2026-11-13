package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
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
			// Runs after the lease is released; bounded so the final export
			// cannot outlast Cloud Run's shutdown grace period.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := otelCollector.Shutdown(shutdownCtx); err != nil {
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
	// GCS state requires ownership before ingestion. A waiting instance serves
	// health checks without opening a Jetstream subscription.
	if err := withIngestionLease(ctx, config.JetstreamStateFile, logger, healthServer, func(intakeCtx, workCtx context.Context) error {
		return runIngestion(intakeCtx, workCtx, config, logger, healthServer, *dryRun, *skipTLSVerify, *noRewind, *maxRewindMinutes)
	}); err != nil {
		logger.Error("Jetstream ingestion failed: %v", err)
		os.Exit(1)
	}
}

// runIngestion uses ctx to stop intake and workCtx to bound draining and writes.
// It must finish its writers before returning so the caller can safely release
// a GCS lease. Local runs use the same cleanup without ownership coordination.
func runIngestion(ctx, workCtx context.Context, config *common.Config, logger *common.IngestLogger, healthServer *common.HealthServer, dryRun, skipTLSVerify, noRewind bool, maxRewindMinutes int) error {
	ctx, stopIntake := context.WithCancel(ctx)
	workCtx, stopWork := context.WithCancel(workCtx)
	var drainTimer *time.Timer
	// Defers run in reverse order. Register this first so the drain deadline
	// remains active while later deferred writer cleanup is still running.
	defer func() {
		if drainTimer != nil {
			drainTimer.Stop()
		}
		stopWork()
	}()
	// Cancellation signals these loops to stop; waiting ensures they have
	// actually finished before ingestion can return.
	var background sync.WaitGroup
	defer func() {
		stopIntake()
		background.Wait()
	}()
	// With GCS state, ownership has already been acquired. Read the cursor now
	// because the previous owner may have advanced it while this instance waited.
	stateManager, err := common.NewStateManagerWithContext(ctx, config.JetstreamStateFile, logger)
	if err != nil {
		return fmt.Errorf("initialize state manager: %w", err)
	}
	defer func() { _ = stateManager.Close() }()

	// Initialize Elasticsearch client
	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}

	esClient, err := common.NewElasticsearchClientWithContext(ctx, esConfig, logger)
	if err != nil {
		return err
	}

	// Ensure period-based indices exist and are the write target for likes,
	// like_tombstones, and posts. Jetstream updates post like counts through the
	// posts alias, so posts must always have a write index as well. Runs at
	// startup and every minute so that period rollovers
	// are detected promptly without waiting for the next batch flush.
	if !dryRun {
		ensureIndices := func() error {
			indexCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			for _, alias := range []string{"likes", "like_tombstones", "posts", "replies"} {
				name := common.CurrentIndexName(alias, config.IndexPeriod)
				if err := common.EnsureIndex(indexCtx, esClient, name, alias, logger); err != nil {
					return fmt.Errorf("failed to ensure index for %s: %w", alias, err)
				}
			}
			return nil
		}

		{
			backoff := time.Second
			for {
				if err := ensureIndices(); err == nil {
					break
				} else {
					logger.Error("ensureIndices failed (retrying in %v): %v", backoff, err)
				}
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
				if backoff < 60*time.Second {
					backoff *= 2
				}
			}
		}

		background.Add(1)
		go func() {
			defer background.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := ensureIndices(); err != nil {
						logger.Error("%v", err)
					}
				}
			}
		}()
	}

	// Initialize and start rate limiter
	threshold := config.LikeRateLimitPerHour / (60 / config.LikeRateLimitWindowMinutes)
	windowDur := time.Duration(config.LikeRateLimitWindowMinutes) * time.Minute
	blockDur := time.Duration(config.LikeBlockDurationMinutes) * time.Minute
	rateLimiter := jetstream_ingest.NewRateLimiter(windowDur, blockDur, threshold)
	rateLimiter.Start(ctx)

	// Follow deltas for the API's per-user followed-users cache (api#83).
	// Entirely optional: with no Firestore project configured, or if the
	// client cannot be built, ingestion carries on exactly as before and the
	// API falls back to its TTL refresh.
	var followWriter *jetstream_ingest.FollowWriter
	var followFirestore *firestore.Client
	if !dryRun && config.FollowCacheEnabled() {
		followFirestore, err = common.NewFirestoreClient(ctx, config.FirestoreProject, config.FirestoreDatabase)
		if err != nil {
			logger.Error("Failed to create Firestore client for follow deltas: %v (continuing without follow-cache updates)", err)
		} else {
			followStore := common.NewFirestoreFollowStore(followFirestore, logger)
			trackedUsers := jetstream_ingest.NewTrackedUsers(followStore, logger)
			background.Add(1)
			go func() {
				defer background.Done()
				trackedUsers.Run(ctx, time.Duration(config.FollowsTrackedRefreshSec)*time.Second)
			}()

			followWriter = jetstream_ingest.NewFollowWriter(followStore, trackedUsers, logger, config.FollowsWriteBuffer)
			go followWriter.Run(workCtx)
			defer func() {
				followWriter.Close()
				<-followWriter.Done()
				if err := followFirestore.Close(); err != nil {
					logger.Error("Failed to close Firestore client: %v", err)
				}
			}()
			logger.Info("Follow-delta writer started (database=%s)", config.FirestoreDatabase)
		}
	}

	// Start blocklist persistence goroutine (writes to GCS periodically)
	if !dryRun && config.BlocklistDestination != "" {
		gcsClient, err := storage.NewClient(ctx)
		if err != nil {
			logger.Error("Failed to create GCS client for blocklist export: %v (continuing without blocklist export)", err)
		} else {
			// Parse gs://bucket/prefix from BlocklistDestination
			dest := strings.TrimPrefix(config.BlocklistDestination, "gs://")
			parts := strings.SplitN(dest, "/", 2)
			blocklistBucket := parts[0]
			blocklistPrefix := ""
			if len(parts) == 2 {
				blocklistPrefix = parts[1]
			}

			type BlockWindow struct {
				Start time.Time `json:"start"`
				End   time.Time `json:"end"`
			}
			var historyMu sync.Mutex
			blocklistHistory := make(map[string][]BlockWindow)

			background.Add(1)
			go func() {
				defer background.Done()
				defer func() {
					if cerr := gcsClient.Close(); cerr != nil {
						logger.Error("Failed to close GCS client: %v", cerr)
					}
				}()
				ticker := time.NewTicker(blockDur / 2)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						snapshot := rateLimiter.GetBlockedAccounts()
						historyMu.Lock()
						for did, entry := range snapshot {
							blocklistHistory[did] = append(blocklistHistory[did], BlockWindow{
								Start: entry.BlockedAt,
								End:   entry.BlockedAt.Add(entry.Duration),
							})
						}
						data, err := json.Marshal(blocklistHistory)
						if err != nil {
							logger.Error("Failed to marshal blocklist: %v", err)
							historyMu.Unlock()
							continue
						}
						ts := time.Now().UTC().Format("20060102_150405")
						objectName := fmt.Sprintf("blocked_accts_%s.json", ts)
						objectPath := fmt.Sprintf("%s/%s", blocklistPrefix, objectName)
						if blocklistPrefix == "" {
							objectPath = objectName
						}
						wc := gcsClient.Bucket(blocklistBucket).Object(objectPath).NewWriter(ctx)
						if _, werr := wc.Write(data); werr != nil {
							logger.Error("Failed to write blocklist to GCS: %v", werr)
							logger.Metric("jetstream.blocklist_export_error_count", 1)
							if cerr := wc.Close(); cerr != nil {
								logger.Error("Failed to close GCS writer after write error: %v", cerr)
							}
							historyMu.Unlock()
							continue
						}
						if werr := wc.Close(); werr != nil {
							logger.Error("Failed to close GCS writer for blocklist: %v", werr)
							logger.Metric("jetstream.blocklist_export_error_count", 1)
							historyMu.Unlock()
							continue
						}
						blocklistHistory = make(map[string][]BlockWindow)
						historyMu.Unlock()
						logger.Debug("Exported blocklist snapshot to gs://%s/%s", blocklistBucket, objectPath)
					}
				}
			}()
		}
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
		return fmt.Errorf("start Jetstream client: %w", err)
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
	cursorWriterDone := make(chan struct{})
	if !dryRun {
		go func() {
			defer close(cursorWriterDone)
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					// Cleanup saves the final cursor after workers finish; they may
					// still advance pendingCursor while draining queued batches.
					return
				case <-ticker.C:
					cursorMu.Lock()
					if hasPendingUpdate {
						if err := stateManager.UpdateCursorContext(ctx, pendingCursor); err != nil {
							logger.Error("Failed to update cursor: %v", err)
							logger.Metric("jetstream.state_update_error_count", 1)
						} else {
							hasPendingUpdate = false
							// Keep the client's reconnection cursor in sync so that
							// WebSocket reconnects resume from the latest processed
							// position rather than replaying from the startup cursor.
							client.UpdateCursor(pendingCursor)
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
	} else {
		close(cursorWriterDone)
	}

	// Posts join the lean two-tower corpus the moment a like pushes them over
	// the threshold (greenearth-social/ingex#442). nil disables promotion.
	var qualityCfg *common.QualityPromotionConfig
	if config.QualityIndexEnabled {
		qualityCfg = &common.QualityPromotionConfig{
			SourceIndex:  "posts",
			Threshold:    config.QualityLikeThreshold,
			IndexPeriod:  config.IndexPeriod,
			RetentionAge: config.QualityRetentionAge,
		}
		logger.Info("Quality corpus promotion enabled (threshold: %d likes, retention: %s)",
			config.QualityLikeThreshold, config.QualityRetentionAge)
	}

	// Like counts on popular posts are summed and written periodically rather
	// than on every like; see common.LikeCountCoalescer. nil writes every like.
	var coalescer *common.LikeCountCoalescer
	flusherDone := make(chan struct{})
	if config.LikeCountFlushInterval > 0 {
		coalescer = common.NewLikeCountCoalescer(config.LikeCountExactBelow, config.LikeCountCacheSize)
		logger.Info("Like-count coalescing enabled (exact below %d likes, flush every %s, cache %d posts)",
			config.LikeCountExactBelow, config.LikeCountFlushInterval, config.LikeCountCacheSize)
		go func() {
			defer close(flusherDone)
			ticker := time.NewTicker(config.LikeCountFlushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Stops sending at shutdown and holds the rest for the
					// final flush; a chunk already sent finishes on workCtx
					// rather than being cancelled mid-request.
					flushLikeCounts(ctx, workCtx, esClient, coalescer, qualityCfg, 1, dryRun, logger)
				}
			}
		}()
	} else {
		close(flusherDone)
		logger.Info("Like-count coalescing disabled: every like is written immediately")
	}

	// Workers use workCtx so queued writes can finish after intake stops,
	// but are canceled when ownership is lost or the drain deadline expires.
	const numWorkers = 10
	workersDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go esWorker(workCtx, i, batchChan, esClient, &cursorMu, &pendingCursor, &hasPendingUpdate, &pendingBatchCount, &pendingSkipCount, dryRun, logger, qualityCfg, coalescer, &wg)
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

			logger.Metric("jetstream.inbound_count", 1)
			msg := common.NewJetstreamMessage(rawMsg, logger)

			// Follow deltas for the API's followed-users cache (api#83).
			// Handled before the sampler deliberately: sampling exists to
			// control *like* volume in stage, and applying it here would
			// silently drop most of a stage user's follows. The writer
			// filters to the users we actually serve, so the volume is
			// negligible either way, and Enqueue drops rather than blocking
			// so this can never stall the ingestion loop.
			if followWriter != nil && (msg.IsFollow() || msg.IsFollowDelete()) {
				// Labelled by whether the event was for one of our users, so
				// "no follow writes" can be told apart from "no follows seen".
				if followWriter.Enqueue(msg) {
					logger.Metric("jetstream.follow_events_count", 1)
				} else {
					logger.Metric("jetstream.follow_events_untracked_count", 1)
				}
				continue
			}

			if !common.ShouldSampleDID(msg.GetAuthorDID(), config.Environment) {
				logger.Metric("jetstream.sample_dropped_count", 1)
				skippedCount++
				continue
			}

			// Handle like deletions
			if msg.IsLikeDelete() {
				if msg.GetAtURI() == "" {
					logger.Error("Skipping like deletion with empty at_uri (author_did: %s)", msg.GetAuthorDID())
					logger.Metric("jetstream.validation_skip_count", 1)
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

				if blocked, newlyBlocked := rateLimiter.RecordLike(msg.GetAuthorDID()); blocked {
					if newlyBlocked {
						logger.Metric("jetstream.blocked_accounts_count", 1)
					}
					logger.Metric("jetstream.dropped_likes_count", 1)
					skippedCount++
					continue
				}

				if msg.GetAtURI() == "" {
					logger.Error("Skipping like with empty at_uri (author_did: %s)", msg.GetAuthorDID())
					logger.Metric("jetstream.validation_skip_count", 1)
					skippedCount++
					continue
				}

				if msg.GetSubjectURI() == "" {
					logger.Error("Skipping like with empty subject_uri (at_uri: %s, author_did: %s)", msg.GetAtURI(), msg.GetAuthorDID())
					logger.Metric("jetstream.validation_skip_count", 1)
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
	}

cleanup:
	stopIntake()
	// Also bound cleanup when the stream ends without a service shutdown.
	// An earlier cancellation from the ownership controller still takes priority.
	drainTimer = time.AfterFunc(ingestionLeaseTiming.drain, stopWork)
	// Stop accepting follow deltas; the deferred cleanup joins the writer.
	if followWriter != nil {
		followWriter.Close()
	}

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
		case <-workCtx.Done():
			logger.Error("Stopped before final like batch could be queued")
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

		likeDocs, err := common.BulkGetLikes(workCtx, esClient, "likes", likeIDs, logger)
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
		case <-workCtx.Done():
			logger.Error("Stopped before final delete batch could be queued")
		}
	}

	// Closing the channel lets workers drain buffered jobs, then finish.
	// workCtx can still cancel the drain before all queued jobs are processed.
	close(batchChan)

	// Wait for all workers to complete
	<-workersDone

	<-flusherDone
	if coalescer != nil {
		if workCtx.Err() == nil {
			flushLikeCounts(workCtx, workCtx, esClient, coalescer, qualityCfg, shutdownFlushConcurrency, dryRun, logger)
		}
		// Whatever is still held now is lost when the process exits. Prod
		// holds ~25 000 posts per flush interval, so this is the number to
		// watch if a deploy ever again runs out of drain time.
		if pending, _ := coalescer.Stats(); pending > 0 {
			logger.Metric("like_counts.flush_abandoned_count", float64(pending))
			logger.Error("Shut down with like counts for %d posts unwritten", pending)
		}
	}

	// After workers and the periodic writer finish, pendingCursor is no longer
	// changing. Only checkpoint while workCtx still permits writes; lease loss
	// cancels it without granting a graceful drain period.
	<-cursorWriterDone
	if !dryRun && hasPendingUpdate && workCtx.Err() == nil {
		if err := stateManager.UpdateCursorContext(workCtx, pendingCursor); err != nil {
			logger.Error("Failed to flush final cursor update: %v", err)
			logger.Metric("jetstream.state_update_error_count", 1)
		}
	}

	logger.Info("Jetstream ingestion complete. Processed: %d, Deleted: %d, Skipped: %d", processedCount, deletedCount, skippedCount)
	return nil
}

// writeLikeCounts applies like-count deltas to posts and replies — a subject
// is one or the other, and the update 404s harmlessly in the index that lacks
// it — then records the new counts and promotes posts that crossed the quality
// threshold. Failures are logged, not returned: like counts and the quality
// corpus are derived data, and must never stall like ingestion.
func writeLikeCounts(ctx context.Context, esClient *elasticsearch.Client, updates []common.LikeCountUpdate, coalescer *common.LikeCountCoalescer, qualityCfg *common.QualityPromotionConfig, dryRun bool, logger *common.IngestLogger) {
	if len(updates) == 0 {
		return
	}
	logger.Metric("like_counts.written_count", float64(len(updates)))

	var wg sync.WaitGroup
	var replyResults []common.LikeCountResult
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		if replyResults, err = common.BulkUpdateLikeCountsWithResults(ctx, esClient, "replies", updates, dryRun, logger); err != nil {
			logger.Error("Failed to update like counts in replies: %v", err)
		}
	}()
	postResults, err := common.BulkUpdateLikeCountsWithResults(ctx, esClient, "posts", updates, dryRun, logger)
	if err != nil {
		logger.Error("Failed to update like counts in posts: %v", err)
	}
	wg.Wait()

	coalescer.Observe(postResults)
	coalescer.Observe(replyResults)

	if err == nil && qualityCfg != nil && len(postResults) > 0 {
		if _, perr := common.PromoteQualityPosts(ctx, esClient, logger, *qualityCfg, postResults, dryRun); perr != nil {
			logger.Error("Failed to promote posts to the quality corpus: %v", perr)
		}
	}
}

// shutdownFlushConcurrency is how many chunks the final flush writes at once.
//
// The final flush has to fit in the drain (ingestionLeaseTiming.drain, 5s),
// which Cloud Run's 10s SIGTERM-to-SIGKILL window does not let us lengthen. In
// prod it holds up to ~25 000 posts — 250 chunks — and a chunk takes ~50ms
// (p95 ~160ms), so written one at a time it needed 10-25s and was cancelled at
// the deadline, losing the rest on every deploy. Ten at a time is about 2.5s at that worst case. It matches
// numWorkers, whose writes have all finished by then, so it replaces their
// load on Elasticsearch rather than adding to it.
//
// Periodic flushes stay sequential: they are not racing anything, and
// spreading them out keeps their re-indexing off ES's peak.
const shutdownFlushConcurrency = 10

// flushLikeCounts writes every like-count delta the coalescer is holding, in
// chunks, concurrency at a time.
//
// stopCtx ends the flush: no further chunk is sent once it is done, and every
// unsent update goes back to the coalescer (Hold) for the next flush. writeCtx
// bounds the requests themselves. They are separate so the periodic flush can
// stop at SIGTERM without cancelling the chunk it already sent — a cancelled
// bulk update may or may not have been applied, so it can be neither retried
// nor counted as lost — and leave the remainder to the final flush.
func flushLikeCounts(stopCtx, writeCtx context.Context, esClient *elasticsearch.Client, coalescer *common.LikeCountCoalescer, qualityCfg *common.QualityPromotionConfig, concurrency int, dryRun bool, logger *common.IngestLogger) {
	updates := coalescer.Drain()
	_, cached := coalescer.Stats()
	logger.Metric("like_counts.cached_count", float64(cached))
	// Chunked to the size of a worker batch (100 likes), so one flush can't
	// build a request big enough to time out.
	const chunk = 100

	chunks := make(chan []common.LikeCountUpdate)
	var wg sync.WaitGroup
	for range max(concurrency, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chunks {
				writeLikeCounts(writeCtx, esClient, c, coalescer, qualityCfg, dryRun, logger)
			}
		}()
	}
	sent := 0
	// Checked before every send: a select with both cases ready picks at
	// random, and would keep sending after stopCtx is done.
	for sent < len(updates) && stopCtx.Err() == nil {
		end := min(sent+chunk, len(updates))
		select {
		case chunks <- updates[sent:end]:
			sent = end
		case <-stopCtx.Done():
		}
	}
	close(chunks)
	wg.Wait()

	logger.Metric("like_counts.flushed_count", float64(sent))
	if unsent := updates[sent:]; len(unsent) > 0 {
		coalescer.Hold(unsent)
		logger.Info("Like-count flush stopped after %d of %d posts; holding the rest", sent, len(updates))
		return
	}
	logger.Info("Flushed held like counts for %d posts (%d counts cached)", len(updates), cached)
}

// esWorker processes batches of documents and writes them to Elasticsearch
func esWorker(ctx context.Context, id int, batchChan <-chan batchJob, esClient *elasticsearch.Client, cursorMu *sync.Mutex, pendingCursor *int64, hasPendingUpdate *bool, pendingBatchCount *int, pendingSkipCount *int, dryRun bool, logger *common.IngestLogger, qualityCfg *common.QualityPromotionConfig, coalescer *common.LikeCountCoalescer, wg *sync.WaitGroup) {
	defer wg.Done()

	batchCounter := 0
	for job := range batchChan {
		// Buffered jobs may remain after cancellation; do not start another write.
		if ctx.Err() != nil {
			return
		}
		batchCounter++
		// Calculate freshness once at start
		freshnessSeconds := common.CalculateFreshness(job.timeUs)
		logger.Metric("freshness_sec", float64(freshnessSeconds))
		logger.Metric("freshness_sec_gauge", float64(freshnessSeconds))
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

						writeLikeCounts(ctx, esClient, coalescer.Route(updates), coalescer, qualityCfg, dryRun, logger)
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

				writeLikeCounts(ctx, esClient, coalescer.Route(updates), coalescer, qualityCfg, dryRun, logger)
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
