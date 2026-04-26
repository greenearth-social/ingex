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

	// Test data setup
	testData := []struct {
		uri string
		did string
	}{
		{"at://did:plc:abc123/app.bsky.feed.post/1", "did:plc:abc123"},
		{"at://did:plc:def456/app.bsky.feed.post/2", "did:plc:def456"},
		{"at://did:plc:xyz789/app.bsky.feed.post/3", "did:plc:xyz789"},
	}

	// Create temporary directory for test data
	tmpDir, err := os.MkdirTemp("", "megastream_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	for _, td := range testData {
		content := fmt.Sprintf(`{"message":{"commit":{"operation":"create","record":{"text":"test post","createdAt":"2024-01-01T00:00:00Z"}}},"did":"%s"}`, td.did)
		filePath := filepath.Join(tmpDir, fmt.Sprintf("%s.json", strings.ReplaceAll(td.uri, ":", "_")))
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}
	}

	// Set up ingestion config
	config := megastream_ingest.Config{
		InputDir:           tmpDir,
		NumWorkers:         2,
		CommitInterval:     1 * time.Second,
		Elasticsearch:      esConfig,
		BadMessagesDir:     filepath.Join(tmpDir, "bad"),
		MaxMessageSize:     1024 * 1024,
		ProcessingTimeout:  30 * time.Second,
		ReplayMode:         false,
		ReplayStart:        time.Time{},
		ReplayEnd:          time.Time{},