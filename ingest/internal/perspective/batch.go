package perspective

import (
	"context"
	"errors"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/greenearth/ingest/internal/common"
)

// ScoreInput is a single post to score.
type ScoreInput struct {
	AtURI   string // correlation ID
	Content string
}

// Outcome classifies what happened to one input, so callers can tell the three
// cases apart that all look like "no score" from the outside.
type Outcome int

const (
	// OutcomeScored means Scores and Combined are populated.
	OutcomeScored Outcome = iota
	// OutcomeUnsupportedLanguage means the API declined to score the text.
	// Expected, and permanent: re-submitting the same text will not help, so
	// the document is still stamped with perspective_scored_at.
	OutcomeUnsupportedLanguage
	// OutcomeNoContent means the post has no text to score — an image- or
	// video-only post. Permanent in the same way an unsupported language is,
	// and stamped for the same reason: without a stamp these posts stay in
	// the backfill's scan forever, and since they sort oldest-first they
	// crowd out the posts that actually need scoring.
	OutcomeNoContent
	// OutcomeSkipped means the quota policy refused a slot. Leaves the
	// document unstamped so a backfill picks it up.
	OutcomeSkipped
	// OutcomeFailed means the request was made and errored. Also leaves the
	// document unstamped.
	OutcomeFailed
)

// ScoreResult is the outcome for a single input, in input order.
type ScoreResult struct {
	AtURI    string
	Outcome  Outcome
	Scores   map[string]float64 // raw attribute scores, keyed by storage key
	Combined float64            // PRC score in ScoreBounds
	Err      error
}

// BatchScorer fans requests out over a set of posts with bounded concurrency,
// behind the shared-quota rate limiter.
//
// Perspective has no batch endpoint, so this is one HTTP request per post —
// unlike internal/inference, where a chunk is a single call. Concurrency and
// the rate limit therefore both apply per post, and failures isolate per post
// rather than per chunk.
type BatchScorer struct {
	client         *Client
	limiter        *limiter
	maxConcurrency int
	logger         *common.IngestLogger
}

// NewBatchScorer creates a BatchScorer. qps is this process's share of the
// Perspective quota, which is shared with the api's serving path.
func NewBatchScorer(client *Client, qps, maxConcurrency int, policy QuotaPolicy, logger *common.IngestLogger) *BatchScorer {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return &BatchScorer{
		client:         client,
		limiter:        newLimiter(qps, policy, logger),
		maxConcurrency: maxConcurrency,
		logger:         logger,
	}
}

// Score scores every input, returning results in input order.
//
// It never returns an error: per-post failures are reported in the result's
// Outcome and Err. Scoring is advisory, and a Perspective outage must not stop
// posts reaching Elasticsearch.
func (b *BatchScorer) Score(ctx context.Context, inputs []ScoreInput) []ScoreResult {
	results := make([]ScoreResult, len(inputs))
	if len(inputs) == 0 {
		return results
	}

	start := time.Now()

	var group errgroup.Group
	group.SetLimit(b.maxConcurrency)

	for i, input := range inputs {
		group.Go(func() error {
			results[i] = b.scoreOne(ctx, input)
			// Always nil: one post's failure never cancels the rest.
			return nil
		})
	}
	_ = group.Wait()

	b.logger.Metric("perspective.batch.duration_ms", float64(time.Since(start).Milliseconds()))
	return results
}

func (b *BatchScorer) scoreOne(ctx context.Context, input ScoreInput) ScoreResult {
	result := ScoreResult{AtURI: input.AtURI}

	if strings.TrimSpace(input.Content) == "" {
		result.Outcome = OutcomeNoContent
		return result
	}

	if err := b.limiter.acquire(ctx); err != nil {
		result.Outcome = OutcomeSkipped
		result.Err = err
		return result
	}

	scores, err := b.client.Score(ctx, input.Content)
	switch {
	case err == nil:
		result.Outcome = OutcomeScored
		result.Scores = scores
		result.Combined = PRCScore(scores)
		b.logger.Metric("perspective.score.count", 1)
	case errors.Is(err, ErrLanguageNotSupported):
		result.Outcome = OutcomeUnsupportedLanguage
		b.logger.Metric("perspective.score.unsupported_language.count", 1)
	default:
		result.Outcome = OutcomeFailed
		result.Err = err
		b.logger.Metric("perspective.score.failed.count", 1)
		b.logger.Debug("Perspective scoring failed for %s: %v", input.AtURI, err)
	}
	return result
}
