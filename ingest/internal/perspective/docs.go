package perspective

import (
	"context"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// AttachPerspectiveScores scores docs and sets their perspective fields in
// place. No-op when b is nil, which is how the kill switch is expressed.
//
// Fail-open, like inference.AttachPostTowerEmbeddings: a post whose scoring
// failed, or which the quota policy refused a slot, is left with no
// perspective fields at all, so it still indexes, the api falls back to
// scoring it live, and cmd/backfill_perspective can pick it up later.
//
// Posts that can never be scored — no text at all, or a language the API
// declines to rate — are stamped with perspective_scored_at and no score. That
// distinction is what makes both the backfill and the api converge: an
// unstamped post is retried forever, which for the permanently unscorable is
// pure waste.
//
// Returns per-outcome counts for the caller to log.
func AttachPerspectiveScores(ctx context.Context, b *BatchScorer, docs []common.PostDoc) (scored, unscorable, skipped, failed int) {
	if b == nil || len(docs) == 0 {
		return 0, 0, 0, 0
	}

	inputs := make([]ScoreInput, len(docs))
	for i, doc := range docs {
		inputs[i] = ScoreInput{AtURI: doc.AtURI, Content: doc.Content}
	}

	results := b.Score(ctx, inputs)
	if len(results) != len(docs) {
		// Score contracts to return exactly one result per input, in order,
		// and the fields below are attached by position. If that ever stopped
		// holding, every post in the batch would silently receive some other
		// post's toxicity score — far worse than no score at all.
		b.logger.Error("Perspective returned %d results for %d posts; skipping the batch", len(results), len(docs))
		return 0, 0, 0, len(docs)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for i := range docs {
		doc := &docs[i]
		switch results[i].Outcome {
		case OutcomeScored:
			combined := results[i].Combined
			doc.PerspectiveScores = results[i].Scores
			doc.CombinedPerspectiveScore = &combined
			doc.PerspectiveScoredAt = now
			scored++
		case OutcomeUnsupportedLanguage, OutcomeNoContent:
			// Stamped but unscored: permanently unscorable, so neither the
			// backfill nor the api should ever ask about it again.
			doc.PerspectiveScoredAt = now
			unscorable++
		case OutcomeSkipped:
			skipped++
		case OutcomeFailed:
			failed++
		}
	}

	b.logger.Metric("posts.perspective.scored.count", float64(scored))
	b.logger.Metric("posts.perspective.unscorable.count", float64(unscorable))
	b.logger.Metric("posts.perspective.skipped.count", float64(skipped))
	b.logger.Metric("posts.perspective.failed.count", float64(failed))

	return scored, unscorable, skipped, failed
}
