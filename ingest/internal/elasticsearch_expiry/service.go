package elasticsearch_expiry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
)

// Collection represents an Elasticsearch index collection to clean up
type Collection struct {
	IndexAlias string // The alias name (e.g., "posts", "likes", "post_tombstones")
	DateField  string // The date field to filter on (e.g., "created_at", "deleted_at")
}

// Config holds configuration for the expiry service
type Config struct {
	CutoffDate time.Time // Documents older than this date will be deleted
	DryRun     bool      // If true, only count documents without deleting
}

// Service handles expiration of documents from Elasticsearch
type Service struct {
	client *elasticsearch.Client
	config Config
	logger *common.IngestLogger
}

// NewService creates a new expiry service
func NewService(client *elasticsearch.Client, config Config, logger *common.IngestLogger) *Service {
	return &Service{
		client: client,
		config: config,
		logger: logger,
	}
}

// ExpireCollection removes expired documents from a specific collection
func (s *Service) ExpireCollection(ctx context.Context, collection Collection) (int, error) {
	s.logger.Info("Starting expiry for collection: %s", collection.IndexAlias)

	if s.config.DryRun {
		// In dry-run mode, count documents that would be deleted
		return s.countExpiredDocuments(ctx, collection)
	}

	// Use Delete By Query API for efficient deletion
	return s.deleteExpiredDocuments(ctx, collection)
}

// countExpiredDocuments counts how many documents would be deleted (for dry-run mode)
func (s *Service) countExpiredDocuments(ctx context.Context, collection Collection) (int, error) {
	cutoffDateStr := s.config.CutoffDate.Format(time.RFC3339)

	// Build the count query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				collection.DateField: map[string]interface{}{
					"lt": cutoffDateStr,
				},
			},
		},
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal count query: %w", err)
	}

	s.logger.Debug("Count query for %s: %s", collection.IndexAlias, string(queryJSON))

	// Execute the count
	res, err := s.client.Count(
		s.client.Count.WithContext(ctx),
		s.client.Count.WithIndex(collection.IndexAlias),
		s.client.Count.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			s.logger.Error("Failed to close count response body: %v", err)
		}
	}()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("count request failed: %s - %s", res.Status(), string(body))
	}

	// Parse the response
	var response struct {
		Count int `json:"count"`
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("failed to parse count response: %w", err)
	}

	s.logger.Info("Dry-run: Would delete %d documents from %s", response.Count, collection.IndexAlias)
	return response.Count, nil
}

// deleteExpiredDocuments uses the Delete By Query API to efficiently delete expired documents
func (s *Service) deleteExpiredDocuments(ctx context.Context, collection Collection) (int, error) {
	cutoffDateStr := s.config.CutoffDate.Format(time.RFC3339)

	// Build the delete by query request
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				collection.DateField: map[string]interface{}{
					"lt": cutoffDateStr,
				},
			},
		},
		// Add conflicts handling - proceed even if there are version conflicts
		"conflicts": "proceed",
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal delete query: %w", err)
	}

	s.logger.Debug("Delete by query for %s: %s", collection.IndexAlias, string(queryJSON))

	// Execute the delete by query
	res, err := s.client.DeleteByQuery(
		[]string{collection.IndexAlias},
		strings.NewReader(string(queryJSON)),
		s.client.DeleteByQuery.WithContext(ctx),
		s.client.DeleteByQuery.WithWaitForCompletion(true), // Wait for operation to complete
		s.client.DeleteByQuery.WithRefresh(true),           // Refresh indices after deletion
		s.client.DeleteByQuery.WithTimeout(5*time.Minute),  // Set timeout for the operation
	)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete by query: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			s.logger.Error("Failed to close delete by query response body: %v", err)
		}
	}()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("delete by query request failed: %s - %s", res.Status(), string(body))
	}

	// Parse the response
	var response struct {
		Deleted          int   `json:"deleted"`
		VersionConflicts int   `json:"version_conflicts"`
		TimedOut         bool  `json:"timed_out"`
		Took             int   `json:"took"`
		Failures         []any `json:"failures"`
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("failed to parse delete by query response: %w", err)
	}

	// Log operation details
	s.logger.Info("Delete by query completed for %s: deleted=%d, took=%dms, conflicts=%d",
		collection.IndexAlias, response.Deleted, response.Took, response.VersionConflicts)

	if response.TimedOut {
		s.logger.Error("Delete by query timed out for %s", collection.IndexAlias)
	}

	if len(response.Failures) > 0 {
		s.logger.Error("Delete by query had %d failures for %s", len(response.Failures), collection.IndexAlias)
		for i, failure := range response.Failures {
			s.logger.Error("Failure %d: %v", i+1, failure)
		}
	}

	return response.Deleted, nil
}
