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

	"github.com/greenearth/ingest/internal/common"
	"github.com/parquet-go/parquet-go"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no file writes)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	outputPath := flag.String("output-path", "", "Override PARQUET_OUTPUT_PATH env var")
	indexName := flag.String("index", "posts", "Elasticsearch index to export")
	maxRecords := flag.Int64("max-records", 0, "Maximum records per file (0=unlimited)")
	fetchSize := flag.Int("fetch-size", 1000, "Batch size for ES queries")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - Elasticsearch Export Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no files will be written")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, finishing current batch...")
		cancel()
	}()

	logger.Info("Starting export from index: %s", *indexName)
	if err := runExport(ctx, config, logger, *dryRun, *skipTLSVerify, *outputPath, *indexName, *maxRecords, *fetchSize); err != nil {
		logger.Error("Export failed: %v", err)
		os.Exit(1)
	}

	logger.Info("Export completed successfully")
}

func runExport(ctx context.Context, config *common.Config, logger *common.IngestLogger,
	dryRun, skipTLSVerify bool, outputPath, indexName string,
	maxRecordsPerFile int64, fetchSize int) error {

	if config.ElasticsearchURL == "" {
		return fmt.Errorf("ELASTICSEARCH_URL environment variable is required")
	}

	if outputPath == "" {
		outputPath = config.ParquetOutputPath
	}
	if outputPath == "" {
		return fmt.Errorf("output path not specified (use --output-path or PARQUET_OUTPUT_PATH)")
	}

	if !dryRun {
		if err := os.MkdirAll(outputPath, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		return fmt.Errorf("failed to create ES client: %w", err)
	}

	if maxRecordsPerFile <= 0 {
		maxRecordsPerFile = config.ParquetMaxRecords
	}

	var fileNum int = 1
	var totalRecords int64 = 0
	var afterCreatedAt, afterIndexedAt string
	var currentFileBatch []common.ExtractPost

	for {
		select {
		case <-ctx.Done():
			if len(currentFileBatch) > 0 && !dryRun {
				if err := writeParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
					logger.Error("Failed to write final parquet file: %v", err)
				}
			}
			return ctx.Err()
		default:
		}

		response, err := common.FetchPosts(ctx, esClient, logger, indexName, afterCreatedAt, afterIndexedAt, fetchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch posts: %w", err)
		}

		if len(response.Hits.Hits) == 0 {
			logger.Info("No more records to fetch")
			break
		}

		batchPosts := common.HitsToExtractPosts(response.Hits.Hits)
		currentFileBatch = append(currentFileBatch, batchPosts...)
		totalRecords += int64(len(batchPosts))

		logger.Info("Fetched %d records (total: %d)", len(batchPosts), totalRecords)

		if maxRecordsPerFile > 0 && int64(len(currentFileBatch)) >= maxRecordsPerFile {
			if !dryRun {
				if err := writeParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
					return fmt.Errorf("failed to write parquet file: %w", err)
				}
				fileNum++
			} else {
				lastPost := currentFileBatch[len(currentFileBatch)-1]
				filename := generateFilename(indexName, lastPost.RecordCreatedAt)
				logger.Info("Dry-run: Would write %s with %d records", filename, len(currentFileBatch))
				fileNum++
			}
			currentFileBatch = currentFileBatch[:0]
		}

		lastHit := response.Hits.Hits[len(response.Hits.Hits)-1]
		afterCreatedAt = lastHit.Source.CreatedAt
		afterIndexedAt = lastHit.Source.IndexedAt
	}

	if len(currentFileBatch) > 0 {
		if !dryRun {
			if err := writeParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
				return fmt.Errorf("failed to write final parquet file: %w", err)
			}
		} else {
			lastPost := currentFileBatch[len(currentFileBatch)-1]
			filename := generateFilename(indexName, lastPost.RecordCreatedAt)
			logger.Info("Dry-run: Would write final %s with %d records", filename, len(currentFileBatch))
		}
	}

	logger.Info("Export complete: %d total records in %d files", totalRecords, fileNum)
	return nil
}

func generateFilename(indexName, lastPostTimestamp string) string {
	// Parse the timestamp to extract date/time
	// Expected format: "2025-10-12T09:05:56.961Z" or similar RFC3339
	t, err := time.Parse(time.RFC3339, lastPostTimestamp)
	if err != nil {
		// Fallback to current time if parsing fails
		t = time.Now().UTC()
	}

	// Format: bsky_posts_YYYYMMDD_HHMMSS.parquet or bsky_likes_YYYYMMDD_HHMMSS.parquet
	indexType := "posts"
	if strings.Contains(indexName, "like") {
		indexType = "likes"
	}

	return fmt.Sprintf("bsky_%s_%s.parquet", indexType, t.Format("20060102_150405"))
}

func writeParquetFile(basePath string, indexName string, posts []common.ExtractPost, logger *common.IngestLogger) error {
	if len(posts) == 0 {
		return fmt.Errorf("no posts to write")
	}

	// Use the last post's timestamp for the filename (posts are sorted by created_at)
	lastPost := posts[len(posts)-1]
	filename := generateFilename(indexName, lastPost.RecordCreatedAt)
	fullPath := filepath.Join(basePath, filename)

	logger.Info("Writing %d records to: %s", len(posts), fullPath)

	if err := parquet.WriteFile(fullPath, posts); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	logger.Info("Successfully wrote %d records to %s", len(posts), fullPath)
	return nil
}
