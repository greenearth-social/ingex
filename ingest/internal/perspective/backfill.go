package perspective

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/greenearth/ingest/internal/common"
)

// BackfillConfig parameterises a backfill pass.
type BackfillConfig struct {
	// SourceIndex is scanned for unscored posts, normally posts_recent.
	SourceIndex string
	// Window bounds the scan to posts created within this age. Zero scans the
	// whole index.
	Window time.Duration
	// PageSize is the search_after page size. Zero means 500.
	PageSize int
	// MaxPosts caps how many posts a single run will score. Zero means no cap.
	MaxPosts int
	// Now is the reference time for the window. Zero means time.Now().
	Now time.Time
}

// BackfillStats reports what a backfill pass did.
type BackfillStats struct {
	Scanned int
	Scored  int
	// Unscorable counts posts stamped but not scored: no text, or a language
	// the API declines to rate. They are written, so they leave the scan.
	Unscorable int
	Skipped    int
	Failed     int
	Updated    int
	Pages      int
}

// Backfill scores posts that have never been submitted to the Perspective API.
//
// It exists because scoring at ingest only ever covers posts ingested after the
// feature shipped, and because posts are left unscored whenever the quota
// policy is "skip" or a request fails. Both leave gaps that the api would
// otherwise paper over by scoring live, one feed request at a time, forever.
//
// The pass is resumable and safe to re-run: writes are keyed by at_uri, and the
// scan selects on the absence of perspective_scored_at, so posts scored by an
// earlier run are not revisited.
//
// It shares the scorer — and therefore the rate limiter — with everything else
// in the process, so a backfill cannot outrun the quota slice ingest was given.
// MaxPosts is the blunter protection: a backfill over a large window would
// otherwise run for hours, and being able to bound one run makes it something
// you can try.
func Backfill(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *common.IngestLogger,
	scorer *BatchScorer,
	cfg BackfillConfig,
	dryRun bool,
) (BackfillStats, error) {
	var stats BackfillStats

	now := cfg.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}

	var windowStart string
	if cfg.Window > 0 {
		windowStart = now.Add(-cfg.Window).UTC().Format(time.RFC3339)
	}

	var after common.PostScanCursor
	for {
		if err := ctx.Err(); err != nil {
			return stats, nil //nolint:nilerr // cancellation is a clean stop, not a failure; the caller reports partial stats
		}

		size := pageSize
		if cfg.MaxPosts > 0 && cfg.MaxPosts-stats.Scanned < size {
			size = cfg.MaxPosts - stats.Scanned
		}
		if size <= 0 {
			break
		}

		posts, cursor, err := common.FetchUnscoredPosts(
			ctx, client, logger, cfg.SourceIndex, windowStart, after, size,
		)
		if err != nil {
			return stats, fmt.Errorf("scan for unscored posts: %w", err)
		}
		if cursor.HitCount == 0 {
			break
		}

		stats.Pages++
		stats.Scanned += cursor.HitCount

		inputs := make([]ScoreInput, len(posts))
		for i, post := range posts {
			inputs[i] = ScoreInput{AtURI: post.AtURI, Content: post.Content}
		}

		scoredAt := time.Now().UTC().Format(time.RFC3339)
		updates := make([]common.PerspectiveUpdate, 0, len(posts))
		for _, result := range scorer.Score(ctx, inputs) {
			switch result.Outcome {
			case OutcomeScored:
				combined := result.Combined
				updates = append(updates, common.PerspectiveUpdate{
					AtURI:         result.AtURI,
					Scores:        result.Scores,
					CombinedScore: &combined,
					ScoredAt:      scoredAt,
				})
				stats.Scored++
			case OutcomeUnsupportedLanguage, OutcomeNoContent:
				updates = append(updates, common.PerspectiveUpdate{
					AtURI:    result.AtURI,
					ScoredAt: scoredAt,
				})
				stats.Unscorable++
			case OutcomeSkipped:
				stats.Skipped++
			case OutcomeFailed:
				stats.Failed++
			}
		}

		updated, err := common.BulkUpdatePerspectiveScores(ctx, client, cfg.SourceIndex, updates, dryRun, logger)
		if err != nil {
			return stats, fmt.Errorf("write perspective scores: %w", err)
		}
		stats.Updated += updated

		logger.Debug("Backfill page %d: %d scanned, %d scored, %d unscorable, %d skipped, %d failed, %d written",
			stats.Pages, len(posts), stats.Scored, stats.Unscorable, stats.Skipped, stats.Failed, updated)

		// A page that came back short of what was asked for is the last one.
		if cursor.HitCount < size {
			break
		}
		if cursor.CreatedAt == "" || cursor.IndexedAt == "" || cursor.AtURI == "" {
			// Without a complete cursor the next page would repeat this one
			// forever, or skip past documents sharing the last sort key.
			logger.Error("Backfill stopping: page %d returned an incomplete sort cursor", stats.Pages)
			break
		}
		after = cursor
	}

	return stats, nil
}
