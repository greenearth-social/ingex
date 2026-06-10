// Package inference provides a client for the Green Earth inference service
// and utilities for computing post-tower embeddings during ingestion. It is
// shared between ingest services so the post embedding pipeline can migrate
// from megastream_ingest to jetstream_ingest without changes.
package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

const postTowerPredictPath = "/models/post-tower/predict"

// ClientConfig configures the inference service client
type ClientConfig struct {
	BaseURL        string        // e.g. https://inference-stage.greenearth.social; empty disables the client
	APIKey         string //nolint:gosec // G117: struct field name, not a secret value; sent as the X-API-Key header
	Timeout        time.Duration // per-request HTTP timeout
	MaxRetries     int           // retries beyond the first attempt
	RetryBaseDelay time.Duration // base delay for exponential backoff
}

// Client is an HTTP client for the inference service. Construct one per
// process with NewClient and share it; the underlying http.Client pools
// connections.
type Client struct {
	httpClient *http.Client
	config     ClientConfig
	logger     *common.IngestLogger
}

// NewClient creates a new inference service client
func NewClient(config ClientConfig, logger *common.IngestLogger) *Client {
	if config.RetryBaseDelay <= 0 {
		config.RetryBaseDelay = 200 * time.Millisecond
	}
	return &Client{
		httpClient: &http.Client{Timeout: config.Timeout},
		config:     config,
		logger:     logger,
	}
}

type postTowerRequest struct {
	PostEmbeddings   [][]float32 `json:"post_embeddings"`
	TargetAuthorDIDs []string    `json:"target_author_dids"`
}

type postTowerResponse struct {
	Outputs   [][]float32 `json:"outputs"`
	ModelType string      `json:"model_type"`
}

// PostTowerPredict computes post-tower embeddings for a batch of content
// embeddings and their author DIDs. Outputs are returned in input order.
// Retries transport errors, 429s and 5xx responses with exponential backoff;
// other 4xx responses fail immediately. The batch must not exceed the
// server's GE_INFERENCE_MAX_BATCH.
func (c *Client) PostTowerPredict(ctx context.Context, postEmbeddings [][]float32, authorDIDs []string) ([][]float32, error) {
	if len(postEmbeddings) != len(authorDIDs) {
		return nil, fmt.Errorf("input length mismatch: %d embeddings vs %d author DIDs", len(postEmbeddings), len(authorDIDs))
	}
	if len(postEmbeddings) == 0 {
		return [][]float32{}, nil
	}

	body, err := json.Marshal(postTowerRequest{
		PostEmbeddings:   postEmbeddings,
		TargetAuthorDIDs: authorDIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	start := time.Now()
	c.logger.Metric("inference.request.count", 1)

	outputs, err := c.doWithRetries(ctx, body)
	c.logger.Metric("inference.request.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		c.logger.Metric("inference.request.errors", 1)
		return nil, err
	}

	if len(outputs) != len(postEmbeddings) {
		c.logger.Metric("inference.request.errors", 1)
		return nil, fmt.Errorf("output count mismatch: got %d outputs for %d inputs", len(outputs), len(postEmbeddings))
	}

	return outputs, nil
}

// doWithRetries performs the HTTP request, retrying retryable failures with
// exponential backoff and jitter
func (c *Client) doWithRetries(ctx context.Context, body []byte) ([][]float32, error) {
	var lastErr error
	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := c.config.RetryBaseDelay * (1 << (attempt - 1))
			jitter := time.Duration(rand.Int63n(int64(delay) + 1)) //nolint:gosec // G404: jitter does not need crypto randomness
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay + jitter):
			}
		}

		outputs, retryable, err := c.doOnce(ctx, body)
		if err == nil {
			return outputs, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
		c.logger.Debug("Inference request attempt %d failed (retryable): %v", attempt+1, err)
	}
	return nil, fmt.Errorf("inference request failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// doOnce performs a single HTTP request. The second return value indicates
// whether the failure is retryable.
func (c *Client) doOnce(ctx context.Context, body []byte) ([][]float32, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.BaseURL+postTowerPredictPath, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.config.APIKey)

	resp, err := c.httpClient.Do(req) //nolint:gosec // G704: BaseURL comes from service configuration, not user input
	if err != nil {
		return nil, ctx.Err() == nil, fmt.Errorf("inference request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() // Ignore error in cleanup
	}()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return nil, retryable, fmt.Errorf("inference service returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed postTowerResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, fmt.Errorf("failed to decode response: %w", err)
	}

	return parsed.Outputs, false, nil
}
