package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/jetstream_ingest"
)

func main() {
	// Parse command line flags
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no writes to Elasticsearch)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - BlueSky Jetstream Ingest Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no writes to Elasticsearch")
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
	runIngestion(ctx, config, logger, *dryRun, *skipTLSVerify)
}

func runIngestion(ctx context.Context, config *common.Config, logger *common.IngestLogger, dryRun, skipTLSVerify bool) {
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
	if err := client.Start(ctx); err != nil {
		logger.Error("Failed to start Jetstream client: %v", err)
		os.Exit(1)
	}
	defer client.Close()

	// Process messages from Jetstream
	msgChan := client.GetMessageChannel()
	var batch []common.LikeDoc
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

			if msg.GetURI() == "" {
				logger.Error("Skipping like with empty URI (author_did: %s)", msg.GetAuthorDID())
				skippedCount++
				continue
			}

			if msg.GetSubjectURI() == "" {
				logger.Error("Skipping like with empty subject URI (uri: %s, author_did: %s)", msg.GetURI(), msg.GetAuthorDID())
				skippedCount++
				continue
			}

			doc := common.CreateLikeDoc(msg)
			batch = append(batch, doc)

			if len(batch) >= batchSize {
				if err := common.BulkIndexLikes(ctx, esClient, "likes", batch, dryRun, logger); err != nil {
					logger.Error("Failed to bulk index likes: %v", err)
				} else {
					processedCount += len(batch)
					if dryRun {
						logger.Info("Dry-run: Would index batch: %d likes (total: %d, skipped: %d)", len(batch), processedCount, skippedCount)
					} else {
						logger.Info("Indexed batch: %d likes (total: %d, skipped: %d)", len(batch), processedCount, skippedCount)
					}
				}
				batch = batch[:0]
			}
		}
	}

cleanup:
	// Index remaining documents in batch
	if len(batch) > 0 {
		if err := common.BulkIndexLikes(ctx, esClient, "likes", batch, dryRun, logger); err != nil {
			logger.Error("Failed to bulk index final batch: %v", err)
		} else {
			processedCount += len(batch)
			if dryRun {
				logger.Info("Dry-run: Would index final batch: %d likes", len(batch))
			} else {
				logger.Info("Indexed final batch: %d likes", len(batch))
			}
		}
	}

	logger.Info("Jetstream ingestion complete. Processed: %d, Skipped: %d", processedCount, skippedCount)
}
