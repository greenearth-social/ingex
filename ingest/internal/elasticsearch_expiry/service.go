package elasticsearch_expiry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
)

// Retry policy for transient Elasticsearch unavailability.
//
// A node replacement takes the cluster RED for tens of seconds while shards
// recover, and every request touching those shards fails until it finishes.
// The go-elasticsearch transport does retry 5xx, but with no backoff, so all
// of its attempts land within a few milliseconds of each other and the job
// exits non-zero over a hiccup that resolves on its own. Spacing the retries
// out here rides the recovery window out instead.
const (
	maxAttempts       = 6
	initialRetryDelay = 2 * time.Second
	maxRetryDelay     = 32 * time.Second
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

	// Retry backoff, seeded from the package defaults; tests shorten them.
	initialRetryDelay time.Duration
	maxRetryDelay     time.Duration
}

// NewService creates a new expiry service
func NewService(client *elasticsearch.Client, config Config, logger *common.IngestLogger) *Service {
	return &Service{
		client:            client,
		config:            config,
		logger:            logger,
		initialRetryDelay: initialRetryDelay,
		maxRetryDelay:     maxRetryDelay,
	}
}

// ExpireCollection removes expired documents from a specific collection.
// Transient Elasticsearch failures are retried with exponential backoff;
// anything else fails immediately.
func (s *Service) ExpireCollection(ctx context.Context, collection Collection) (int, error) {
	s.logger.Info("Starting expiry for collection: %s", collection.IndexAlias)

	attempt := func() (int, error) {
		if s.config.DryRun {
			// In dry-run mode, count documents that would be deleted
			return s.countExpiredDocuments(ctx, collection)
		}

		// Use Delete By Query API for efficient deletion
		return s.deleteExpiredDocuments(ctx, collection)
	}

	delay := s.initialRetryDelay
	for i := 1; ; i++ {
		count, err := attempt()
		if err == nil || !isTransient(err) || i == maxAttempts {
			return count, err
		}

		s.logger.Error("Attempt %d/%d for %s hit a transient Elasticsearch failure, retrying in %v: %v",
			i, maxAttempts, collection.IndexAlias, delay, err)
		s.logger.Metric("expiry.retry_count", 1)

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}

		if delay *= 2; delay > s.maxRetryDelay {
			delay = s.maxRetryDelay
		}
	}
}

// transientError marks a failure worth retrying: Elasticsearch was unreachable,
// or reported that it could not serve the request right now.
type transientError struct {
	err error
}

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

func isTransient(err error) bool {
	var t *transientError
	return errors.As(err, &t)
}

// isTransientStatus reports whether an Elasticsearch response status means
// "try again later" rather than "this request is wrong".
//
// 401 is deliberately included. NewElasticsearchClient authenticates before
// this service ever runs, and the API key is injected once at task start, so a
// 401 mid-run does not mean the credentials are bad — it means Elasticsearch
// could not read its own security index to verify them, which clears up when
// the cluster recovers.
func isTransientStatus(code int) bool {
	return code == http.StatusUnauthorized || code == http.StatusTooManyRequests || code >= 500
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
		// Transport-level failure: the cluster is unreachable, not the request wrong.
		return 0, &transientError{fmt.Errorf("failed to execute count query: %w", err)}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			s.logger.Error("Failed to close count response body: %v", err)
		}
	}()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		countErr := fmt.Errorf("count request failed: %s - %s", res.Status(), string(body))
		if isTransientStatus(res.StatusCode) {
			return 0, &transientError{countErr}
		}
		return 0, countErr
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
		// Transport-level failure: the cluster is unreachable, not the request wrong.
		return 0, &transientError{fmt.Errorf("failed to execute delete by query: %w", err)}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			s.logger.Error("Failed to close delete by query response body: %v", err)
		}
	}()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		deleteErr := fmt.Errorf("delete by query request failed: %s - %s", res.Status(), string(body))
		if isTransientStatus(res.StatusCode) {
			return 0, &transientError{deleteErr}
		}
		return 0, deleteErr
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
		s.logger.Metric("expiry.timed_out_count", 1)
	}

	if len(response.Failures) > 0 {
		s.logger.Error("Delete by query had %d failures for %s", len(response.Failures), collection.IndexAlias)
		s.logger.Metric("expiry.bulk_failures_count", float64(len(response.Failures)))
		for i, failure := range response.Failures {
			s.logger.Error("Failure %d: %v", i+1, failure)
		}
	}

	return response.Deleted, nil
}
