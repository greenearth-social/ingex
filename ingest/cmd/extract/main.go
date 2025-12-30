package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no file writes)")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (use for local development only)")
	outputPath := flag.String("output-path", "", "Override GE_PARQUET_DESTINATION env var")
	windowSizeMin := flag.Int("window-size-min", 0, "Time window in minutes from now (e.g., 240 for 4-hour lookback). Overrides start-time and end-time if set.")
	startTime := flag.String("start-time", "", "Start time for export window (RFC3339 format, e.g., 2025-01-01T00:00:00Z)")
	endTime := flag.String("end-time", "", "End time for export window (RFC3339 format, e.g., 2025-12-31T23:59:59Z)")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	logger.Info("Green Earth Ingex - Elasticsearch Export Service")
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no files will be written")
	}

	// Calculate time window if --window-size-min is provided
	if *windowSizeMin > 0 {
		now := time.Now().UTC()
		calculatedEndTime := now.Format(time.RFC3339)
		calculatedStartTime := now.Add(-time.Duration(*windowSizeMin) * time.Minute).Format(time.RFC3339)

		logger.Info("Using window size: %d minutes (from %s to %s)",
			*windowSizeMin, calculatedStartTime, calculatedEndTime)

		// Override any provided start/end times
		*startTime = calculatedStartTime
		*endTime = calculatedEndTime
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
			func() string {
				if *startTime != "" {
					return *startTime
				}
				return "beginning"
			}(),
			func() string {
				if *endTime != "" {
					return *endTime
				}
				return "end"
			}())
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
		logger.Error("No indices specified in GE_EXTRACT_INDICES environment variable")
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
		return fmt.Errorf("GE_ELASTICSEARCH_URL environment variable is required")
	}

	// Determine output destination (priority: flag > GE_PARQUET_DESTINATION)
	if outputPath == "" && config.ParquetDestination != "" {
		outputPath = config.ParquetDestination
	}
	if outputPath == "" {
		return fmt.Errorf("output path not specified (use --output-path, GE_PARQUET_DESTINATION)")
	}

	// Check if GCS destination
	isGCS := strings.HasPrefix(outputPath, "gs://")
	var gcsClient *storage.Client
	var gcsBucket, gcsPrefix string

	if isGCS {
		// Parse GCS path: gs://bucket/prefix
		path := strings.TrimPrefix(outputPath, "gs://")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 1 {
			return fmt.Errorf("invalid GCS path: %s (expected gs://bucket/path)", outputPath)
		}
		gcsBucket = parts[0]
		if len(parts) == 2 {
			gcsPrefix = parts[1]
			if !strings.HasSuffix(gcsPrefix, "/") {
				gcsPrefix += "/"
			}
		}

		if !dryRun {
			var err error
			gcsClient, err = storage.NewClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create GCS client: %w", err)
			}
			defer func() {
				if err := gcsClient.Close(); err != nil {
					logger.Error("Failed to close GSC client: %v", err)
				}
			}()
		}

		logger.Info("Using GCS destination: gs://%s/%s", gcsBucket, gcsPrefix)
	} else {
		// For local destinations, create directory
		if !dryRun {
			if err := os.MkdirAll(outputPath, 0750); err != nil {
				return fmt.Errorf("failed to create output directory: %w", err)
			}
		}
		logger.Info("Using local destination: %s", outputPath)
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

		indexType := getIndexType(indexName, logger)

		var exportErr error
		switch indexType {
		case IndexTypePosts:
			exportErr = runExportForPosts(ctx, esClient, logger, dryRun, outputPath, isGCS, gcsClient, gcsBucket, gcsPrefix, indexName, startTime, endTime, config)
		case IndexTypeLikes:
			exportErr = runExportForLikes(ctx, esClient, logger, dryRun, outputPath, isGCS, gcsClient, gcsBucket, gcsPrefix, indexName, startTime, endTime, config)
		case IndexTypeUnknown:
			logger.Error("Skipping index %s: unknown index type", indexName)
			continue
		default:
			logger.Error("Unhandled index type for index %s", indexName)
			continue
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
	dryRun bool, outputPath string, isGCS bool, gcsClient *storage.Client, gcsBucket, gcsPrefix, indexName, startTime, endTime string, config *common.Config) error {

	const writeChunkSize = 5000

	maxRecordsPerFile := config.ParquetMaxRecords
	fetchSize := config.ExtractFetchSize

	var totalRecords int64 = 0
	var afterCreatedAt, afterIndexedAt string
	var writerState *parquetWriterState[common.ExtractPost]
	var writeBuffer []common.ExtractPost

	defer func() {
		if writerState != nil && !dryRun {
			if len(writeBuffer) > 0 {
				if err := writerState.writeChunk(writeBuffer, logger); err != nil {
					logger.Error("Failed to write final chunk: %v", err)
				}
			}
			if err := writerState.close(ctx, outputPath, indexName, logger); err != nil {
				logger.Error("Failed to close parquet file: %v", err)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown signal received during export")
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
		totalRecords += int64(len(batchPosts))

		logger.Info("Fetched %d records (total: %d)", len(batchPosts), totalRecords)

		if writerState == nil && !dryRun {
			writerState, err = openParquetWriterForPosts(ctx, outputPath, isGCS, gcsClient, gcsBucket, gcsPrefix, indexName, logger)
			if err != nil {
				return fmt.Errorf("failed to open parquet writer: %w", err)
			}
			writeBuffer = make([]common.ExtractPost, 0, writeChunkSize)
		}

		if !dryRun {
			writeBuffer = append(writeBuffer, batchPosts...)

			if len(batchPosts) > 0 {
				writerState.lastRecordTimestamp = batchPosts[len(batchPosts)-1].RecordCreatedAt
			}

			for len(writeBuffer) >= writeChunkSize {
				chunk := writeBuffer[:writeChunkSize]
				if err := writerState.writeChunk(chunk, logger); err != nil {
					return fmt.Errorf("failed to write chunk: %w", err)
				}
				writeBuffer = writeBuffer[writeChunkSize:]
			}

			if maxRecordsPerFile > 0 && writerState.recordsInCurrentFile >= maxRecordsPerFile {
				if len(writeBuffer) > 0 {
					if err := writerState.writeChunk(writeBuffer, logger); err != nil {
						return fmt.Errorf("failed to flush write buffer: %w", err)
					}
					writeBuffer = writeBuffer[:0]
				}

				if err := writerState.close(ctx, outputPath, indexName, logger); err != nil {
					return fmt.Errorf("failed to close parquet file: %w", err)
				}

				writerState = nil
			}
		} else {
			if len(batchPosts) > 0 {
				lastPost := batchPosts[len(batchPosts)-1]
				filename := generateFilename(indexName, lastPost.RecordCreatedAt, logger)
				logger.Info("Dry-run: Would process %s", filename)
			}
		}

		lastHit := response.Hits.Hits[len(response.Hits.Hits)-1]
		afterCreatedAt = lastHit.Source.CreatedAt
		afterIndexedAt = lastHit.Source.IndexedAt
	}

	logger.Info("Export complete: %d total records", totalRecords)
	return nil
}

func runExportForLikes(ctx context.Context, esClient *elasticsearch.Client, logger *common.IngestLogger,
	dryRun bool, outputPath string, isGCS bool, gcsClient *storage.Client, gcsBucket, gcsPrefix, indexName, startTime, endTime string, config *common.Config) error {

	const writeChunkSize = 5000

	maxRecordsPerFile := config.ParquetMaxRecords
	fetchSize := config.ExtractFetchSize

	var totalRecords int64 = 0
	var afterCreatedAt, afterIndexedAt string
	var writerState *parquetWriterState[common.ExtractLike]
	var writeBuffer []common.ExtractLike

	defer func() {
		if writerState != nil && !dryRun {
			if len(writeBuffer) > 0 {
				if err := writerState.writeChunk(writeBuffer, logger); err != nil {
					logger.Error("Failed to write final chunk: %v", err)
				}
			}
			if err := writerState.close(ctx, outputPath, indexName, logger); err != nil {
				logger.Error("Failed to close parquet file: %v", err)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown signal received during export")
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
		totalRecords += int64(len(batchLikes))

		logger.Info("Fetched %d records (total: %d)", len(batchLikes), totalRecords)

		if writerState == nil && !dryRun {
			writerState, err = openParquetWriterForLikes(ctx, outputPath, isGCS, gcsClient, gcsBucket, gcsPrefix, indexName, logger)
			if err != nil {
				return fmt.Errorf("failed to open parquet writer: %w", err)
			}
			writeBuffer = make([]common.ExtractLike, 0, writeChunkSize)
		}

		if !dryRun {
			writeBuffer = append(writeBuffer, batchLikes...)

			if len(batchLikes) > 0 {
				writerState.lastRecordTimestamp = batchLikes[len(batchLikes)-1].RecordCreatedAt
			}

			for len(writeBuffer) >= writeChunkSize {
				chunk := writeBuffer[:writeChunkSize]
				if err := writerState.writeChunk(chunk, logger); err != nil {
					return fmt.Errorf("failed to write chunk: %w", err)
				}
				writeBuffer = writeBuffer[writeChunkSize:]
			}

			if maxRecordsPerFile > 0 && writerState.recordsInCurrentFile >= maxRecordsPerFile {
				if len(writeBuffer) > 0 {
					if err := writerState.writeChunk(writeBuffer, logger); err != nil {
						return fmt.Errorf("failed to flush write buffer: %w", err)
					}
					writeBuffer = writeBuffer[:0]
				}

				if err := writerState.close(ctx, outputPath, indexName, logger); err != nil {
					return fmt.Errorf("failed to close parquet file: %w", err)
				}

				writerState = nil
			}
		} else {
			if len(batchLikes) > 0 {
				lastLike := batchLikes[len(batchLikes)-1]
				filename := generateFilename(indexName, lastLike.RecordCreatedAt, logger)
				logger.Info("Dry-run: Would process %s", filename)
			}
		}

		lastHit := response.Hits.Hits[len(response.Hits.Hits)-1]
		afterCreatedAt = lastHit.Source.CreatedAt
		afterIndexedAt = lastHit.Source.IndexedAt
	}

	logger.Info("Export complete: %d total records", totalRecords)
	return nil
}

func generateFilename(indexName, lastPostTimestamp string, logger *common.IngestLogger) string {
	// Parse the timestamp to extract date/time
	// Expected format: "2025-10-12T09:05:56.961Z" or similar RFC3339
	t, err := time.Parse(time.RFC3339, lastPostTimestamp)
	if err != nil {
		// Fallback to current time if parsing fails
		t = time.Now().UTC()
	}

	// Format: bsky_posts_YYYYMMDD_HHMMSS.parquet or bsky_likes_YYYYMMDD_HHMMSS.parquet
	indexType := getIndexType(indexName, logger)

	var typeStr string
	switch indexType {
	case IndexTypePosts:
		typeStr = "posts"
	case IndexTypeLikes:
		typeStr = "likes"
	case IndexTypeUnknown:
		typeStr = "unknown"
	default:
		typeStr = "unknown"
	}

	return fmt.Sprintf("bsky_%s_%s.parquet", typeStr, t.Format("20060102_150405"))
}

// IndexType represents the type of index being exported
type IndexType string

const (
	IndexTypePosts   IndexType = "posts"
	IndexTypeLikes   IndexType = "likes"
	IndexTypeUnknown IndexType = ""
)

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

// ParseIndexType attempts to parse an index name into an IndexType
func ParseIndexType(indexName string) (IndexType, error) {
	lowerName := strings.ToLower(indexName)

	if strings.Contains(lowerName, "post") {
		return IndexTypePosts, nil
	}

	if strings.Contains(lowerName, "like") {
		return IndexTypeLikes, nil
	}

	return IndexTypeUnknown, fmt.Errorf("index name '%s' does not contain 'posts' or 'likes'", indexName)
}

func getIndexType(indexName string, logger *common.IngestLogger) IndexType {
	indexType, err := ParseIndexType(indexName)
	if err != nil {
		logger.Error("Unable to determine index type: %v", err)
	}
	return indexType
}

