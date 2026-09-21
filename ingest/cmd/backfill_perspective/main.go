// Command backfill_perspective scores posts that carry no Perspective result.
//
// Scoring happens at ingest (megastream_ingest), so this covers the two gaps
// that leaves: posts ingested before that shipped, and posts the ingest path
// skipped or failed to score — which is the expected outcome whenever
// GE_PERSPECTIVE_ON_QUOTA is "skip" and the shared quota is under pressure.
// Watch perspective.rate_limit.skipped.count to know when a run is owed.
//
// Safe to re-run and safe to interrupt: it selects on the absence of
// perspective_scored_at, and writes are partial updates keyed by at_uri.
//
// This writes to the posts index only. The quality corpus carries its own copy
// of the combined score (it is what two-tower kNN candidates are read back
// from), and that copy is made at promotion time — so a backfill over posts
// leaves already-promoted documents unscored. Run backfill_quality_index
// afterwards to refresh them, or two-tower candidates keep arriving at the api
// uncached however complete posts looks.
//
// See greenearth-social/api#368.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/perspective"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Scan and score without writing any documents")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (local development only)")
	sourceIndex := flag.String("source-index", "posts_recent", "Index to scan for unscored posts")
	windowHours := flag.Int("window-hours", 0, "Only scan posts created within this many hours (0 = the whole index)")
	pageSize := flag.Int("page-size", 500, "search_after page size")
	maxPosts := flag.Int("max-posts", 0, "Stop after scanning this many posts (0 = no limit)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)
	logger.SetDebugEnabled(*debug)

	if config.ElasticsearchURL == "" {
		logger.Error("GE_ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}
	if !config.PerspectiveEnabled() {
		logger.Error("GE_PERSPECTIVE_API_KEY environment variable is required")
		os.Exit(1)
	}

	policy, err := perspective.ParseQuotaPolicy(config.PerspectiveOnQuota)
	if err != nil {
		logger.Error("GE_PERSPECTIVE_ON_QUOTA: %v", err)
		os.Exit(1)
	}

	esClient, err := common.NewElasticsearchClient(common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: *skipTLSVerify || config.ElasticsearchTLSSkipVerify,
	}, logger)
	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	}

	scorer := perspective.NewBatchScorer(
		perspective.NewClient(perspective.ClientConfig{
			Host:       config.PerspectiveHost,
			APIKey:     config.PerspectiveAPIKey,
			Timeout:    config.PerspectiveTimeout,
			MaxRetries: config.PerspectiveRetryMax,
		}, logger),
		config.PerspectiveQPS, config.PerspectiveMaxConcurrency, policy, logger,
	)

	window := time.Duration(*windowHours) * time.Hour

	logger.Info("Green Earth Ingex - Perspective Backfill")
	logger.Info("Source: %s | window: %s | %d qps | on-quota: %s | max posts: %d",
		*sourceIndex, windowDescription(window), config.PerspectiveQPS, policy, *maxPosts)
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - posts will be scored but nothing written")
	}

	// Ctrl-C stops cleanly after the page in flight. The run is resumable
	// because scored posts drop out of the scan on the next invocation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Shutdown signal received; stopping after the current page...")
		cancel()
	}()

	start := time.Now()
	stats, err := perspective.Backfill(ctx, esClient, logger, scorer, perspective.BackfillConfig{
		SourceIndex: *sourceIndex,
		Window:      window,
		PageSize:    *pageSize,
		MaxPosts:    *maxPosts,
	}, *dryRun)

	logger.Info("Backfill finished in %s: %d scanned, %d scored, %d unscorable, %d skipped, %d failed, %d written, %d pages",
		time.Since(start).Round(time.Second),
		stats.Scanned, stats.Scored, stats.Unscorable, stats.Skipped, stats.Failed, stats.Updated, stats.Pages)

	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	}

	// Skipped and failed posts stay unscored, so say so plainly rather than
	// letting a clean exit imply full coverage.
	if stats.Skipped > 0 || stats.Failed > 0 {
		logger.Info("%d posts remain unscored (%d skipped, %d failed); re-run to retry them",
			stats.Skipped+stats.Failed, stats.Skipped, stats.Failed)
	}
}

func windowDescription(window time.Duration) string {
	if window <= 0 {
		return "whole index"
	}
	return window.String()
}
