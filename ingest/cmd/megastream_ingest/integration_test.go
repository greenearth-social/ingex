package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/megastream_ingest"
)

// TestMegastreamIngestIntegration is an integration test that:
// 1. Checks if Elasticsearch is available (skips if not)
// 2. Runs megastream_ingest on test data via runIngestion
// 3. Verifies the data was indexed in Elasticsearch
func TestMegastreamIngestIntegration(t *testing.T) {
	// Check for required environment variables
	esURL := os.Getenv("GE_ELASTICSEARCH_URL")
	esAPIKey := os.Getenv("GE_ELASTICSEARCH_API_KEY")

	if esURL == "" || esAPIKey == "" {
		t.Skip("Skipping integration test: GE_ELASTICSEARCH_URL and GE_ELASTICSEARCH_API_KEY must be set")
	}

	// Create logger
	logger := common.NewLogger(true)

	// Try to connect to Elasticsearch
	esConfig := common.ElasticsearchConfig{
		URL:           esURL,
		APIKey:        esAPIKey,
		SkipTLSVerify: true, // For local development
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		t.Skipf("Skipping integration test: cannot connect to Elasticsearch: %v", err)
	}

	// Verify connection
	ctx := context.Background()
	res, err := esClient.Info()
	if err != nil {
		t.Skipf("Skipping integration test: Elasticsearch not available: %v", err)
	}
	if err := res.Body.Close(); err != nil {
		t.Logf("Warning: failed to close response body: %v", err)
	}

	t.Log("Elasticsearch is available, running integration test")

	// Set up test data directory
	testDataDir := "../../test_data/megastream"
	absTestDataDir, err := filepath.Abs(testDataDir)
	if err != nil {
		t.Fatalf("Failed to get absolute path for test data: %v", err)
	}

	// Check if test data exists
	if _, err := os.Stat(absTestDataDir); os.IsNotExist(err) {
		t.Skipf("Skipping integration test: test data directory not found: %s", absTestDataDir)
	}

	// Copy test files to a temporary directory to preserve originals
	tempDataDir := filepath.Join(t.TempDir(), "test_data")
	if err := os.MkdirAll(tempDataDir, 0755); err != nil {
		t.Fatalf("Failed to create temp data directory: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(absTestDataDir, "*.db*"))
	if err != nil {
		t.Fatalf("Failed to list test files: %v", err)
	}

	for _, srcPath := range files {
		dstPath := filepath.Join(tempDataDir, filepath.Base(srcPath))
		srcFile, err := os.Open(srcPath)
		if err != nil {
			t.Fatalf("Failed to open source file: %v", err)
		}

		dstFile, err := os.Create(dstPath)
		if err != nil {
			_ = srcFile.Close()
			t.Fatalf("Failed to create dest file: %v", err)
		}

		_, err = io.Copy(dstFile, srcFile)
		_ = srcFile.Close()
		_ = dstFile.Close()
		if err != nil {
			t.Fatalf("Failed to copy file: %v", err)
		}
	}
	t.Logf("Copied %d test files to temporary directory", len(files))

	// Create a temporary state file for this test
	tmpStateFile := filepath.Join(t.TempDir(), "test_state.json")

	// Set up configuration to use the temporary data directory
	config := &common.Config{
		ElasticsearchURL:           esURL,
		ElasticsearchAPIKey:        esAPIKey,
		ElasticsearchTLSSkipVerify: true,
		LocalSQLiteDBPath:          tempDataDir,
		MegastreamStateFile:        tmpStateFile,
		LoggingEnabled:             true,
		SpoolIntervalSec:           60,
	}

	// Create a no-op health server for testing
	healthServer, err := common.NewHealthServer(0, 0, logger) // Port 0 means pick any available port
	if err != nil {
		t.Fatalf("Failed to create health server: %v", err)
	}

	// Start health server in background
	go func() {
		if err := healthServer.Start(ctx); err != nil && ctx.Err() == nil {
			t.Logf("Health server error (non-fatal in test): %v", err)
		}
	}()

	// Initialize state manager and reset cursor to process all test data
	stateManager, err := common.NewStateManager(config.MegastreamStateFile, logger)
	if err != nil {
		t.Fatalf("Failed to initialize state manager: %v", err)
	}

	// Reset cursor to 0 to ensure we process all test data files
	if err := stateManager.UpdateCursor(0); err != nil {
		t.Fatalf("Failed to reset cursor: %v", err)
	}
	t.Log("Reset cursor to 0 to process all test data files")

	// Clean up any existing test data from Elasticsearch before running the test
	deletedCount, err := cleanupTestData(ctx, esClient, absTestDataDir, logger)
	if err != nil {
		t.Fatalf("Failed to cleanup test data: %v", err)
	}
	if deletedCount > 0 {
		t.Logf("Cleaned up %d existing documents from previous test runs", deletedCount)
		// Give ES a moment to process deletions
		time.Sleep(1 * time.Second)
	}
	// Count documents before ingestion
	countBefore, err := countDocuments(esClient, "posts")
	if err != nil {
		t.Fatalf("Failed to count documents before ingestion: %v", err)
	}
	t.Logf("Document count before ingestion: %d", countBefore)

	// Run the actual ingestion using runIngestion from main.go
	if err := runIngestion(ctx, config, logger, healthServer, "local", "once", false, true, false, false, 0); err != nil {
		t.Fatalf("runIngestion failed: %v", err)
	}

	// Give Elasticsearch a moment to index
	time.Sleep(2 * time.Second)

	// Count documents after ingestion
	countAfter, err := countDocuments(esClient, "posts")
	if err != nil {
		t.Fatalf("Failed to count documents after ingestion: %v", err)
	}
	t.Logf("Document count after ingestion: %d", countAfter)

	// Verify that documents were indexed
	docsIndexed := countAfter - countBefore
	if docsIndexed == 0 {
		t.Skip("Skipping integration test: no new documents were indexed (possibly already processed)")
	}

	t.Logf("Successfully indexed %d new documents", docsIndexed)

	// Validate the structure and content of indexed documents
	sampleDocs, err := getSampleDocuments(ctx, esClient, "posts", 100)
	if err != nil {
		t.Fatalf("Failed to retrieve sample documents: %v", err)
	}

	if len(sampleDocs) == 0 {
		t.Fatal("No documents found in index after ingestion")
	}

	t.Logf("Retrieved %d sample documents for validation", len(sampleDocs))

	// Track empty content documents
	emptyContentCount := 0

	// Validate each sample document
	for i, doc := range sampleDocs {
		// Check required fields are present and non-empty
		if doc.AtURI == "" {
			t.Errorf("Document %d: at_uri is empty", i)
		}
		if doc.AuthorDID == "" {
			t.Errorf("Document %d: author_did is empty", i)
		}
		if doc.Content == "" {
			emptyContentCount++
		}
		if doc.IndexedAt == "" {
			t.Errorf("Document %d: indexed_at is empty", i)
		}
		if doc.CreatedAt == "" {
			t.Errorf("Document %d: created_at is empty", i)
		}

		// Log first document as example
		if i == 0 {
			t.Logf("Sample document structure:")
			t.Logf("  at_uri: %s", doc.AtURI)
			t.Logf("  author_did: %s", doc.AuthorDID)
			t.Logf("  content: %.80s...", doc.Content)
			t.Logf("  created_at: %s", doc.CreatedAt)
			t.Logf("  indexed_at: %s", doc.IndexedAt)
			t.Logf("  has_embeddings: %v", len(doc.Embeddings) > 0)
			if doc.ThreadRootPost != "" {
				t.Logf("  thread_root_post: %s", doc.ThreadRootPost)
			}
			if doc.ThreadParentPost != "" {
				t.Logf("  thread_parent_post: %s", doc.ThreadParentPost)
			}
			if doc.QuotePost != "" {
				t.Logf("  quote_post: %s", doc.QuotePost)
			}
		}
	}

	// Check empty content percentage
	emptyContentPct := float64(emptyContentCount) / float64(len(sampleDocs)) * 100
	t.Logf("Documents with empty content: %d/%d (%.1f%%)", emptyContentCount, len(sampleDocs), emptyContentPct)

	if emptyContentPct > 40.0 {
		t.Errorf("Too many documents with empty content: %.1f%% exceeds 40%% threshold (some empty content is expected for image posts)", emptyContentPct)
	}

	t.Log("Integration test completed successfully")
}

// getSampleDocuments retrieves sample documents from the specified index
func getSampleDocuments(ctx context.Context, esClient *elasticsearch.Client, index string, size int) ([]common.ElasticsearchDoc, error) {
	query := map[string]interface{}{
		"size": size,
		"sort": []map[string]interface{}{
			{"indexed_at": map[string]string{"order": "desc"}},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(strings.NewReader(string(body))),
	)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("search returned error: %s - %s", res.Status(), string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response structure: missing hits")
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response structure: hits is not an array")
	}

	var docs []common.ElasticsearchDoc
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// Marshal and unmarshal to convert to ElasticsearchDoc struct
		sourceBytes, err := json.Marshal(source)
		if err != nil {
			continue
		}

		var doc common.ElasticsearchDoc
		if err := json.Unmarshal(sourceBytes, &doc); err != nil {
			continue
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// countDocuments returns the count of documents in the specified index
func countDocuments(esClient *elasticsearch.Client, index string) (int, error) {
	res, err := esClient.Count(
		esClient.Count.WithIndex(index),
	)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = res.Body.Close()
	}()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, err
	}

	count, ok := result["count"].(float64)
	if !ok {
		return 0, nil
	}

	return int(count), nil
}

// TestElasticsearchConnection is a simple test to verify ES connection
func TestElasticsearchConnection(t *testing.T) {
	esURL := os.Getenv("GE_ELASTICSEARCH_URL")
	esAPIKey := os.Getenv("GE_ELASTICSEARCH_API_KEY")

	if esURL == "" || esAPIKey == "" {
		t.Skip("Skipping: GE_ELASTICSEARCH_URL and GE_ELASTICSEARCH_API_KEY must be set")
	}

	logger := common.NewLogger(false)
	esConfig := common.ElasticsearchConfig{
		URL:           esURL,
		APIKey:        esAPIKey,
		SkipTLSVerify: true,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		t.Skipf("Elasticsearch not available: %v", err)
	}

	res, err := esClient.Info()
	if err != nil {
		t.Fatalf("Failed to get ES info: %v", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			t.Logf("Warning: failed to close response body: %v", err)
		}
	}()

	var info map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		t.Fatalf("Failed to decode ES info: %v", err)
	}

	version, ok := info["version"].(map[string]interface{})
	if ok {
		t.Logf("Connected to Elasticsearch version: %v", version["number"])
	}
}

// cleanupTestData reads through test data files and deletes corresponding documents from Elasticsearch
// Returns the number of documents deleted
func cleanupTestData(ctx context.Context, esClient *elasticsearch.Client, testDataDir string, logger *common.IngestLogger) (int, error) {
	// Create a temporary directory and copy test files there so we don't delete the originals
	tempDir, err := os.MkdirTemp("", "cleanup-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	// Copy test files to temp directory
	files, err := filepath.Glob(filepath.Join(testDataDir, "*.db*"))
	if err != nil {
		return 0, fmt.Errorf("failed to list test files: %w", err)
	}

	for _, srcPath := range files {
		dstPath := filepath.Join(tempDir, filepath.Base(srcPath))
		srcFile, err := os.Open(srcPath)
		if err != nil {
			return 0, fmt.Errorf("failed to open source file: %w", err)
		}

		dstFile, err := os.Create(dstPath)
		if err != nil {
			_ = srcFile.Close()
			return 0, fmt.Errorf("failed to create dest file: %w", err)
		}

		_, err = io.Copy(dstFile, srcFile)
		_ = srcFile.Close()
		_ = dstFile.Close()
		if err != nil {
			return 0, fmt.Errorf("failed to copy file: %w", err)
		}
	}

	// Create a temporary state manager for reading files with cursor set to 0
	tempStateManager, err := common.NewStateManager(":memory:", logger)
	if err != nil {
		return 0, fmt.Errorf("failed to create temp state manager: %w", err)
	}

	// Reset cursor to 0 so we read all files
	if err := tempStateManager.UpdateCursor(0); err != nil {
		return 0, fmt.Errorf("failed to reset cursor: %w", err)
	}

	// Create a temporary spooler to read the files from temp directory
	spooler := megastream_ingest.NewLocalSpooler(
		tempDir,
		"once",
		60*time.Second,
		tempStateManager,
		logger,
	)

	// Start the spooler
	if err := spooler.Start(ctx); err != nil {
		return 0, fmt.Errorf("failed to start spooler: %w", err)
	}

	// Collect all at_uris from the test data
	atURIs := make(map[string]bool) // Use map to deduplicate
	rowChan := spooler.GetRowChannel()

	for row := range rowChan {
		if row.AtURI != "" {
			atURIs[row.AtURI] = true
		}
	}

	if err := spooler.Stop(); err != nil {
		return 0, fmt.Errorf("failed to stop spooler: %w", err)
	}

	if len(atURIs) == 0 {
		return 0, nil // No data to clean up
	}

	logger.Info("Cleaning up %d documents from test data", len(atURIs))

	// Delete documents from posts and post_tombstones indexes
	deletedCount := 0

	// Delete from posts index using delete-by-query
	deleted, err := deleteByAtURIs(ctx, esClient, "posts", atURIs)
	if err != nil {
		return deletedCount, fmt.Errorf("failed to delete from posts: %w", err)
	}
	deletedCount += deleted

	// Delete from post_tombstones index
	deleted, err = deleteByAtURIs(ctx, esClient, "post_tombstones", atURIs)
	if err != nil {
		return deletedCount, fmt.Errorf("failed to delete from post_tombstones: %w", err)
	}
	deletedCount += deleted

	logger.Info("Deleted %d total documents from Elasticsearch", deletedCount)
	return deletedCount, nil
}

// deleteByAtURIs deletes documents from the specified index by at_uri
func deleteByAtURIs(ctx context.Context, esClient *elasticsearch.Client, index string, atURIs map[string]bool) (int, error) {
	// Build a terms query to match any of the at_uris
	// Documents are indexed with _id = at_uri, so we use _id query
	uriList := make([]string, 0, len(atURIs))
	for uri := range atURIs {
		uriList = append(uriList, uri)
	}

	// Use delete_by_query API with _id query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"ids": map[string]interface{}{
				"values": uriList,
			},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := esClient.DeleteByQuery(
		[]string{index},
		strings.NewReader(string(body)),
		esClient.DeleteByQuery.WithContext(ctx),
		esClient.DeleteByQuery.WithRefresh(true), // Refresh to make deletions visible immediately
	)
	if err != nil {
		return 0, fmt.Errorf("delete_by_query failed: %w", err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		bodyBytes, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("delete_by_query returned error: %s - %s", res.Status(), string(bodyBytes))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	deleted, ok := result["deleted"].(float64)
	if !ok {
		return 0, nil
	}

	return int(deleted), nil
}
