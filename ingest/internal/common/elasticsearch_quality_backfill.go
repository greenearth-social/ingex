package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// QualityBackfillConfig parameterises a one-time seed of the quality corpus.
type QualityBackfillConfig struct {
	// SourceIndex is scanned for qualifying posts, normally posts_recent.
	SourceIndex string
	// Threshold is the minimum like count for corpus membership.
	Threshold int
	// IndexPeriod controls the period bucket of destination indices.
	IndexPeriod string
	// RetentionAge bounds the scan and mirrors the quality ILM delete age.
	RetentionAge time.Duration
	// Now is the reference time for the window. Zero means time.Now().
	Now time.Time
	// PageSize is the search_after page size. Zero means 500.
	PageSize int
}

// QualityBackfillStats reports what a backfill pass did.
type QualityBackfillStats struct {
	Scanned int
	Indexed int
	Skipped int
	Pages   int
}

// BackfillQualityPosts seeds the quality corpus from SourceIndex.
//
// This deliberately does not use the Elasticsearch _reindex API. The posts
// template excludes "embeddings" from _source, so _reindex — which copies
// _source — would produce documents with no ge_post_embedding at all, and a
// quality index that answers every kNN query with nothing. The vectors exist
// only in doc values, so they have to be read back through docvalue_fields and
// re-indexed explicitly (same trap as greenearth-social/api#325).
//
// Paging uses search_after over (created_at, indexed_at), so a run that dies
// partway can be resumed from the last reported cursor, and re-running is safe:
// documents are written with _id = at_uri, so repeats overwrite in place.
func BackfillQualityPosts(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *IngestLogger,
	cfg QualityBackfillConfig,
	dryRun bool,
) (QualityBackfillStats, error) {
	var stats QualityBackfillStats

	now := cfg.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}

	windowStart := now.Add(-cfg.RetentionAge).UTC().Format(time.RFC3339)

	var afterCreatedAt, afterIndexedAt string
	for {
		resp, err := FetchQualityCandidates(ctx, client, logger, cfg.SourceIndex,
			cfg.Threshold, windowStart, afterCreatedAt, afterIndexedAt, pageSize)
		if err != nil {
			return stats, fmt.Errorf("scan %s at cursor (%q, %q): %w",
				cfg.SourceIndex, afterCreatedAt, afterIndexedAt, err)
		}
		if len(resp.Hits.Hits) == 0 {
			break
		}
		stats.Pages++

		byIndex := make(map[string][]QualityPostDoc)
		for _, hit := range resp.Hits.Hits {
			stats.Scanned++

			doc, ok := qualityDocFromHit(hit)
			if !ok {
				stats.Skipped++
				continue
			}
			index, ok := qualityIndexForPost(doc.CreatedAt, cfg.IndexPeriod, now, cfg.RetentionAge)
			if !ok {
				stats.Skipped++
				continue
			}
			byIndex[index] = append(byIndex[index], doc)
		}

		for index, docs := range byIndex {
			if err := BulkIndex(ctx, client, index, docs, dryRun, logger); err != nil {
				return stats, fmt.Errorf("backfill %d posts into %s (resume from created_at=%q indexed_at=%q): %w",
					len(docs), index, afterCreatedAt, afterIndexedAt, err)
			}
			stats.Indexed += len(docs)
		}

		last := resp.Hits.Hits[len(resp.Hits.Hits)-1]
		afterCreatedAt, afterIndexedAt = last.Source.CreatedAt, last.Source.IndexedAt
		if afterCreatedAt == "" {
			return stats, fmt.Errorf("cannot page past a document with no created_at (id %q)", last.ID)
		}

		logger.Info("Backfill progress: %d scanned, %d indexed, %d skipped (cursor %s)",
			stats.Scanned, stats.Indexed, stats.Skipped, afterCreatedAt)
	}

	return stats, nil
}

// FetchQualityCandidates returns one page of posts eligible for the quality
// corpus: at or above the like threshold and created within the window.
//
// Embeddings come from docvalue_fields for the reason spelled out on
// BackfillQualityPosts.
func FetchQualityCandidates(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *IngestLogger,
	index string,
	minLikeCount int,
	createdAfter string,
	afterCreatedAt string,
	afterIndexedAt string,
	size int,
) (SearchResponse, error) {
	var response SearchResponse

	if size <= 0 {
		size = 500
	}

	filters := []interface{}{
		map[string]interface{}{
			"range": map[string]interface{}{
				"like_count": map[string]interface{}{"gte": minLikeCount},
			},
		},
	}
	if createdAfter != "" {
		filters = append(filters, map[string]interface{}{
			"range": map[string]interface{}{
				"created_at": map[string]interface{}{"gte": createdAfter},
			},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": filters},
		},
		// search_after needs a total order; (created_at, indexed_at) matches
		// FetchPosts and is the pair the resume cursor is expressed in.
		"sort": []interface{}{
			map[string]interface{}{"created_at": "asc"},
			map[string]interface{}{"indexed_at": "asc"},
		},
		"size":            size,
		"docvalue_fields": []interface{}{"embeddings.*"},
	}

	if afterCreatedAt != "" && afterIndexedAt != "" {
		query["search_after"] = []interface{}{afterCreatedAt, afterIndexedAt}
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return response, fmt.Errorf("failed to marshal query: %w", err)
	}

	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	logger.Metric("es.fetch_quality_candidates.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return response, fmt.Errorf("quality candidate search failed: %w", err)
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			logger.Error("Failed to close search response body: %v", cerr)
		}
	}()

	if res.IsError() {
		return response, fmt.Errorf("quality candidate search returned error: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("failed to parse quality candidate response: %w", err)
	}

	logger.Metric("es.fetch_quality_candidates.took_ms", float64(response.Took))
	return response, nil
}
