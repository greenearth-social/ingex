package perspective

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// DefaultHost is Google's Perspective API. GE_PERSPECTIVE_HOST overrides it,
// using the same variable name the api uses, so the devenv stub
// (internal-tools/devenv/perspective-stub) serves both without special cases.
const DefaultHost = "https://commentanalyzer.googleapis.com"

const analyzePath = "/v1alpha1/comments:analyze"

// ClientConfig configures the Perspective API client.
type ClientConfig struct {
	Host           string        // base URL; empty means DefaultHost
	APIKey         string        //nolint:gosec // G117: struct field name, not a secret value; sent as the ?key= query parameter
	Timeout        time.Duration // per-request HTTP timeout
	MaxRetries     int           // retries beyond the first attempt
	RetryBaseDelay time.Duration // base delay for exponential backoff
}

// Client is an HTTP client for the Perspective API. Construct one per process
// and share it; the underlying http.Client pools connections.
type Client struct {
	httpClient *http.Client
	config     ClientConfig
	logger     *common.IngestLogger
}

// NewClient creates a Perspective API client.
func NewClient(config ClientConfig, logger *common.IngestLogger) *Client {
	if config.Host == "" {
		config.Host = DefaultHost
	}
	if config.RetryBaseDelay <= 0 {
		config.RetryBaseDelay = 200 * time.Millisecond
	}
	return &Client{
		httpClient: &http.Client{Timeout: config.Timeout},
		config:     config,
		logger:     logger,
	}
}

// ErrLanguageNotSupported means the API declined to score the text because its
// detected language is not supported by one or more requested attributes.
//
// This is an expected outcome, not a failure: a large share of Bluesky is not
// English. Callers record it as "attempted, unscorable" so the post is not
// re-submitted forever — the same distinction the api draws with
// PerspectiveLanguageNotSupportedError.
var ErrLanguageNotSupported = errors.New("perspective: language not supported")

type analyzeRequest struct {
	Comment             analyzeComment      `json:"comment"`
	RequestedAttributes map[string]struct{} `json:"requestedAttributes"`
	Languages           []string            `json:"languages,omitempty"`
	DoNotStore          bool                `json:"doNotStore"`
}

type analyzeComment struct {
	Text string `json:"text"`
}

type analyzeResponse struct {
	AttributeScores map[string]struct {
		SummaryScore struct {
			Value float64 `json:"value"`
		} `json:"summaryScore"`
	} `json:"attributeScores"`
}

func requestedAttributesPayload() map[string]struct{} {
	payload := make(map[string]struct{}, len(RequestedAttributes))
	for _, name := range RequestedAttributes {
		payload[name] = struct{}{}
	}
	return payload
}

// Score returns the raw attribute scores for text, keyed by the lower-cased
// attribute name (the form they are stored under in Elasticsearch).
//
// Returns ErrLanguageNotSupported for text the API declines to score.
func (c *Client) Score(ctx context.Context, text string) (map[string]float64, error) {
	body, err := json.Marshal(analyzeRequest{
		Comment:             analyzeComment{Text: text},
		RequestedAttributes: requestedAttributesPayload(),
		// Bluesky posts are public, but we have no need for Google to retain
		// them and every reason not to hand over a copy of the firehose.
		DoNotStore: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	start := time.Now()
	scores, err := c.doWithRetries(ctx, body)
	c.logger.Metric("perspective.score.duration_ms", float64(time.Since(start).Milliseconds()))
	return scores, err
}

func (c *Client) doWithRetries(ctx context.Context, body []byte) (map[string]float64, error) {
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

		scores, retryable, err := c.doOnce(ctx, body)
		if err == nil {
			return scores, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
		c.logger.Debug("Perspective request attempt %d failed (retryable): %v", attempt+1, err)
	}
	return nil, fmt.Errorf("perspective request failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// doOnce performs a single request. The second return value reports whether
// the failure is worth retrying.
func (c *Client) doOnce(ctx context.Context, body []byte) (map[string]float64, bool, error) {
	endpoint := c.config.Host + analyzePath + "?key=" + url.QueryEscape(c.config.APIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req) //nolint:gosec // G704: Host comes from service configuration, not user input
	if err != nil {
		return nil, ctx.Err() == nil, fmt.Errorf("perspective request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() // Ignore error in cleanup
	}()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if resp.StatusCode == http.StatusBadRequest && isLanguageNotSupported(respBody) {
			return nil, false, ErrLanguageNotSupported
		}
		// 429 is a genuine quota overrun rather than a transient fault. It is
		// retryable, but the retry is nearly always wasted: our own limiter is
		// what should have prevented it. Count it separately so a rise here
		// reads as "the budget is set too high", not as flaky transport.
		if resp.StatusCode == http.StatusTooManyRequests {
			c.logger.Metric("perspective.score.rate_limited.count", 1)
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return nil, retryable, fmt.Errorf("perspective API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed analyzeResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, fmt.Errorf("failed to decode response: %w", err)
	}

	scores := make(map[string]float64, len(RequestedAttributes))
	for _, name := range RequestedAttributes {
		attr, ok := parsed.AttributeScores[name]
		if !ok {
			// A partial response would silently become a score computed with
			// the missing attribute treated as zero, which for a negatively
			// weighted attribute reads as "not toxic". Refuse it instead.
			return nil, false, fmt.Errorf("perspective response missing attribute %q", name)
		}
		scores[storageKey(name)] = attr.SummaryScore.Value
	}
	return scores, false, nil
}

// isLanguageNotSupported reports whether a 400 body is the expected
// unsupported-language rejection rather than a real client error.
func isLanguageNotSupported(body []byte) bool {
	var parsed struct {
		Error struct {
			Details []struct {
				ErrorType string `json:"errorType"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	for _, detail := range parsed.Error.Details {
		if detail.ErrorType == "LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE" {
			return true
		}
	}
	return false
}
