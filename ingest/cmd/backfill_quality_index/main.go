// Command backfill_quality_index seeds the lean two-tower quality corpus from
// an existing posts index.
//
// Run it once per environment after deploying the posts-quality index template.
// Steady-state membership is maintained by jetstream_ingest, which promotes
// posts as they cross the like threshold; without this backfill the corpus
// would only ever contain posts that crossed *after* that deploy.
//
// See greenearth-social/ingex#442.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Scan and report without writing any documents")
	skipTLSVerify := flag.Bool("skip-tls-verify", false, "Skip TLS certificate verification (local development only)")
	sourceIndex := flag.String("source-index", "posts_recent", "Index to scan for qualifying posts")
	threshold := flag.Int("threshold", 0, "Minimum like count for corpus membership (default: GE_QUALITY_LIKE_THRESHOLD)")
	retentionHours := flag.Int("retention-hours", 0, "Window to backfill, in hours (default: GE_QUALITY_RETENTION_AGE)")
	pageSize := flag.Int("page-size", 500, "search_after page size")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)
	logger.SetDebugEnabled(*debug)

	if config.ElasticsearchURL == "" {
		logger.Error("GE_ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	if *threshold == 0 {
		*threshold = config.QualityLikeThreshold
	}
	retention := config.QualityRetentionAge
	if *retentionHours > 0 {
		retention = time.Duration(*retentionHours) * time.Hour
	}

	logger.Info("Green Earth Ingex - Quality Corpus Backfill")
	logger.Info("Source: %s | threshold: >=%d likes | window: %.1f days | period: %s",
		*sourceIndex, *threshold, retention.Hours()/24.0, config.IndexPeriod)
	if *dryRun {
		logger.Info("Running in DRY-RUN mode - no documents will be written")
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

	// Ctrl-C stops cleanly mid-scan; the run is resumable because documents are
	// written under _id = at_uri, so a rerun overwrites rather than duplicates.
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
	stats, err := common.BackfillQualityPosts(ctx, esClient, logger, common.QualityBackfillConfig{
		SourceIndex:  *sourceIndex,
		Threshold:    *threshold,
		IndexPeriod:  config.IndexPeriod,
		RetentionAge: retention,
		PageSize:     *pageSize,
	}, *dryRun)

	logger.Info("Backfill finished in %s: %d scanned, %d indexed, %d skipped, %d pages",
		time.Since(start).Round(time.Second), stats.Scanned, stats.Indexed, stats.Skipped, stats.Pages)

	if err != nil {
		logger.Error("Backfill failed: %v", err)
		os.Exit(1)
	}
}
