// Package followed_users_backfill walks app.bsky.graph.getFollows for
// tracked users on a schedule and writes the result into the API's
// followed_users_cache Firestore collection — see api/src/app/lib/bsky.py
// and followed_users_cache.py, which this ports the walk/staleness logic
// from. api#453: this replaces the request-triggered background refresh
// that lived in the API process.
package followed_users_backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const followsPageLimit = 100

// userAgent identifies this job to Bluesky's public API. No existing
// User-Agent convention was found elsewhere in ingex, so this follows the
// module's own path (github.com/greenearth/ingest) plus the binary name.
const userAgent = "greenearth-ingex/followed-users-backfill"

// defaultRateLimitBackoff is used on a 429 response with a missing or
// unparseable Retry-After header.
const defaultRateLimitBackoff = 2 * time.Second

// maxRetryAfterDelay caps how long we'll honor a server-provided Retry-After
// value for. Bounds a single worker's wait regardless of what Bluesky sends,
// so a pathological header value can't park a worker indefinitely and so the
// wait stays short enough to be interrupted promptly by ctx cancellation.
const maxRetryAfterDelay = 30 * time.Second

// FollowsResult is the outcome of one walk. Complete is true only when the
// walk ended on its own terms (cursor exhausted or limit reached); false
// means a page error or a context deadline cut it short, and the caller must
// not treat DIDs as authoritative.
type FollowsResult struct {
	DIDs     []string
	Complete bool
}

// BskyClient walks the public Bluesky API for one actor's follows.
type BskyClient struct {
	httpClient       *http.Client
	baseURL          string // overridden in tests; production default set in NewBskyClient
	maxRetries       int
	retryDelay       time.Duration
	rateLimitBackoff time.Duration // fallback backoff when a 429 has no usable Retry-After; overridden in tests
}

// NewBskyClient returns a client using httpClient for transport (share one
// per process; ingex's other clients follow the same pattern — see
// internal/inference/client.go).
func NewBskyClient(httpClient *http.Client) *BskyClient {
	return &BskyClient{
		httpClient:       httpClient,
		baseURL:          "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows",
		maxRetries:       1,
		retryDelay:       100 * time.Millisecond,
		rateLimitBackoff: defaultRateLimitBackoff,
	}
}

type followsPageResponse struct {
	Follows []struct {
		DID string `json:"did"`
	} `json:"follows"`
	Cursor string `json:"cursor"`
}

func (c *BskyClient) getPage(ctx context.Context, actorDID string, pageLimit int, cursor string) (followsPageResponse, error) {
	q := url.Values{}
	q.Set("actor", actorDID)
	q.Set("limit", fmt.Sprintf("%d", pageLimit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"?"+q.Encode(), nil)
		if err != nil {
			return followsPageResponse{}, err
		}
		req.Header.Set("User-Agent", userAgent)
		resp, err := c.httpClient.Do(req) //nolint:gosec // G704: baseURL is a fixed constant, not user input; only query params vary
		if err != nil {
			lastErr = err
			if !isRetryableTransportErr(ctx, err) {
				return followsPageResponse{}, err
			}
			time.Sleep(c.retryDelay)
			continue
		}
		defer func() { _ = resp.Body.Close() }() // Ignore error in cleanup
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("bsky getFollows: status %d", resp.StatusCode)
			if attempt >= c.maxRetries {
				return followsPageResponse{}, lastErr
			}
			var delay time.Duration
			if resp.StatusCode == http.StatusTooManyRequests {
				delay = c.retryAfterDelay(resp)
			} else {
				delay = c.retryDelay
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return followsPageResponse{}, ctx.Err()
			}
			continue
		}
		if resp.StatusCode >= 400 {
			return followsPageResponse{}, fmt.Errorf("bsky getFollows: status %d", resp.StatusCode)
		}
		var page followsPageResponse
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return followsPageResponse{}, fmt.Errorf("decoding bsky getFollows response: %w", err)
		}
		return page, nil
	}
	return followsPageResponse{}, lastErr
}

// retryAfterDelay returns how long to wait before retrying a 429 response.
// Honors the Retry-After header in its seconds form (e.g. "2"); falls back
// to c.rateLimitBackoff if the header is absent or in a form we don't parse
// (e.g. an HTTP-date). Clamped to maxRetryAfterDelay so a large or hostile
// header value can't park a worker for an arbitrarily long time.
func (c *BskyClient) retryAfterDelay(resp *http.Response) time.Duration {
	raw := resp.Header.Get("Retry-After")
	if raw == "" {
		return c.rateLimitBackoff
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs >= 0 {
		return clampRetryAfterDelay(time.Duration(secs) * time.Second)
	}
	return c.rateLimitBackoff
}

// clampRetryAfterDelay bounds a parsed Retry-After duration at maxRetryAfterDelay.
func clampRetryAfterDelay(d time.Duration) time.Duration {
	if d > maxRetryAfterDelay {
		return maxRetryAfterDelay
	}
	return d
}

func isRetryableTransportErr(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false // outer walk context already cancelled/expired — don't retry, let the caller handle it
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// Check if the error wraps context.DeadlineExceeded (per-request context timeout)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// FetchFollows walks actorDID's follows up to limit DIDs. Returns an error
// only when nothing at all could be fetched; any partial result comes back
// with Complete=false instead — mirrors api/src/app/lib/bsky.py::fetch_followed_user_dids.
func (c *BskyClient) FetchFollows(ctx context.Context, actorDID string, limit int) (FollowsResult, error) {
	var dids []string
	var cursor string

	if limit <= 0 {
		return FollowsResult{Complete: true}, nil
	}

	for len(dids) < limit {
		if err := ctx.Err(); err != nil {
			if len(dids) > 0 {
				return FollowsResult{DIDs: dids, Complete: false}, nil
			}
			return FollowsResult{}, fmt.Errorf("fetching follows for %s: %w", actorDID, err)
		}

		pageLimit := followsPageLimit
		if remaining := limit - len(dids); remaining < pageLimit {
			pageLimit = remaining
		}

		page, err := c.getPage(ctx, actorDID, pageLimit, cursor)
		if err != nil {
			if len(dids) > 0 {
				return FollowsResult{DIDs: dids, Complete: false}, nil
			}
			return FollowsResult{}, fmt.Errorf("fetching follows for %s: %w", actorDID, err)
		}

		for _, f := range page.Follows {
			if f.DID != "" {
				dids = append(dids, f.DID)
			}
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}

	return FollowsResult{DIDs: dids[:min(len(dids), limit)], Complete: true}, nil
}
