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

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
	"github.com/parquet-go/parquet-go"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no file writes)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	outputPath := flag.String("output-path", "", "Override PARQUET_OUTPUT_PATH env var")
	startTime := flag.String("start-time", "", "Start time for export window (RFC3339 format, e.g., 2025-01-01T00:00:00Z)")
	endTime := flag.String("end-time", "", "End time for export window (RFC3339 format, e.g., 2025-12-31T23:59:59Z)")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - Elasticsearch Export Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no files will be written")
	}

	// Validate time window if provided
	if *startTime != "" {
		if _, err := time.Parse(time.RFC3339, *startTime); err != nil {
			logger.Error("Invalid start-time format: %v (expected RFC3339, e.g., 2025-01-01T00:00:00Z)", err)
			os.Exit(1)
		}
	}
	if *endTime != "" {
		if _, err := time.Parse(time.RFC3339, *endTime); err != nil {
			logger.Error("Invalid end-time format: %v (expected RFC3339, e.g., 2025-12-31T23:59:59Z)", err)
			os.Exit(1)
		}
	}

	if *startTime != "" || *endTime != "" {
		logger.Info("Time window filter: %s to %s",
			func() string { if *startTime != "" { return *startTime }; return "beginning" }(),
			func() string { if *endTime != "" { return *endTime }; return "end" }())
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

	indices := parseIndices(config.ExtractIndices)
	if len(indices) == 0 {
		logger.Error("No indices specified in EXTRACT_INDICES environment variable")
		os.Exit(1)
	}

	logger.Info("Starting export from %d index(es): %s", len(indices), strings.Join(indices, ", "))
	if err := runExport(ctx, config, logger, *dryRun, *skipTLSVerify, *outputPath, indices, *startTime, *endTime); err != nil {
		logger.Error("Export failed: %v", err)
		os.Exit(1)
	}

	logger.Info("Export completed successfully")
}

func runExport(ctx context.Context, config *common.Config, logger *common.IngestLogger,
	dryRun, skipTLSVerify bool, outputPath string, indices []string, startTime, endTime string) error {

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
		if err := os.MkdirAll(outputPath, 0750); err != nil {
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

	for _, indexName := range indices {
		logger.Info("Starting export from index: %s", indexName)

		indexType := getIndexType(indexName)

		var exportErr error
		if indexType == "likes" {
			exportErr = runExportForLikes(ctx, esClient, logger, dryRun, outputPath, indexName, startTime, endTime, config)
		} else {
			exportErr = runExportForPosts(ctx, esClient, logger, dryRun, outputPath, indexName, startTime, endTime, config)
		}

		if exportErr != nil {
			logger.Error("Failed to export index %s: %v", indexName, exportErr)
			continue
		}

		logger.Info("Completed export from index: %s", indexName)
	}

	return nil
}

func runExportForPosts(ctx context.Context, esClient *elasticsearch.Client, logger *common.IngestLogger,
	dryRun bool, outputPath, indexName, startTime, endTime string, config *common.Config) error {

	maxRecordsPerFile := config.ParquetMaxRecords
	fetchSize := config.ExtractFetchSize

	var fileNum = 1
	var totalRecords int64 = 0
	var afterCreatedAt, afterIndexedAt string
	var currentFileBatch []common.ExtractPost

	for {
		select {
		case <-ctx.Done():
			if len(currentFileBatch) > 0 && !dryRun {
				if err := writePostsParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
					logger.Error("Failed to write final parquet file: %v", err)
				}
			}
			return ctx.Err()
		default:
		}

		response, err := common.FetchPosts(ctx, esClient, logger, indexName, startTime, endTime, afterCreatedAt, afterIndexedAt, fetchSize)
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
				if err := writePostsParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
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
			if err := writePostsParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
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

func runExportForLikes(ctx context.Context, esClient *elasticsearch.Client, logger *common.IngestLogger,
	dryRun bool, outputPath, indexName, startTime, endTime string, config *common.Config) error {

	maxRecordsPerFile := config.ParquetMaxRecords
	fetchSize := config.ExtractFetchSize

	var fileNum = 1
	var totalRecords int64 = 0
	var afterCreatedAt, afterIndexedAt string
	var currentFileBatch []common.ExtractLike

	for {
		select {
		case <-ctx.Done():
			if len(currentFileBatch) > 0 && !dryRun {
				if err := writeLikesParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
					logger.Error("Failed to write final parquet file: %v", err)
				}
			}
			return ctx.Err()
		default:
		}

		response, err := common.FetchLikes(ctx, esClient, logger, indexName, startTime, endTime, afterCreatedAt, afterIndexedAt, fetchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch likes: %w", err)
		}

		if len(response.Hits.Hits) == 0 {
			logger.Info("No more records to fetch")
			break
		}

		batchLikes := common.LikeHitsToExtractLikes(response.Hits.Hits)
		currentFileBatch = append(currentFileBatch, batchLikes...)
		totalRecords += int64(len(batchLikes))

		logger.Info("Fetched %d records (total: %d)", len(batchLikes), totalRecords)

		if maxRecordsPerFile > 0 && int64(len(currentFileBatch)) >= maxRecordsPerFile {
			if !dryRun {
				if err := writeLikesParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
					return fmt.Errorf("failed to write parquet file: %w", err)
				}
				fileNum++
			} else {
				lastLike := currentFileBatch[len(currentFileBatch)-1]
				filename := generateFilename(indexName, lastLike.RecordCreatedAt)
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
			if err := writeLikesParquetFile(outputPath, indexName, currentFileBatch, logger); err != nil {
				return fmt.Errorf("failed to write final parquet file: %w", err)
			}
		} else {
			lastLike := currentFileBatch[len(currentFileBatch)-1]
			filename := generateFilename(indexName, lastLike.RecordCreatedAt)
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

func parseIndices(indicesStr string) []string {
	if indicesStr == "" {
		return []string{}
	}

	indices := strings.Split(indicesStr, ",")
	result := make([]string, 0, len(indices))

	for _, idx := range indices {
		trimmed := strings.TrimSpace(idx)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}

func getIndexType(indexName string) string {
	if strings.Contains(strings.ToLower(indexName), "like") {
		return "likes"
	}
	return "posts"
}

func writePostsParquetFile(basePath string, indexName string, posts []common.ExtractPost, logger *common.IngestLogger) error {
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

func writeLikesParquetFile(basePath string, indexName string, likes []common.ExtractLike, logger *common.IngestLogger) error {
	if len(likes) == 0 {
		return fmt.Errorf("no likes to write")
	}

	lastLike := likes[len(likes)-1]
	filename := generateFilename(indexName, lastLike.RecordCreatedAt)
	fullPath := filepath.Join(basePath, filename)

	logger.Info("Writing %d like records to: %s", len(likes), fullPath)

	if err := parquet.WriteFile(fullPath, likes); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	logger.Info("Successfully wrote %d like records to %s", len(likes), fullPath)
	return nil
}
