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
// Paging uses search_after over (created_at, indexed_at, at_uri). The unique
// post URI breaks timestamp ties so a page boundary cannot skip posts. Re-running
// is safe: documents are written with _id = at_uri, so repeats overwrite in place.
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

	var after []interface{}
	for {
		resp, err := FetchQualityCandidates(ctx, client, logger, cfg.SourceIndex,
			cfg.Threshold, windowStart, after, pageSize)
		if err != nil {
			return stats, fmt.Errorf("scan %s at cursor %v: %w", cfg.SourceIndex, after, err)
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
				return stats, fmt.Errorf("backfill %d posts into %s (cursor %v): %w",
					len(docs), index, after, err)
			}
			stats.Indexed += len(docs)
		}

		last := resp.Hits.Hits[len(resp.Hits.Hits)-1]
		if len(last.Sort) != 3 {
			return stats, fmt.Errorf("cannot page past a document without a complete sort cursor (id %q)", last.ID)
		}
		// Preserve Elasticsearch's exact sort values rather than reconstructing
		// them from _source, where dates can have a different representation.
		after = last.Sort

		logger.Info("Backfill progress: %d scanned, %d indexed, %d skipped (cursor %v)",
			stats.Scanned, stats.Indexed, stats.Skipped, after)
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
	after []interface{},
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
		// Many posts share both timestamps, especially when seeding fixtures.
		// The keyword at_uri field provides a unique, sortable tie-breaker.
		"sort": []interface{}{
			map[string]interface{}{"created_at": "asc"},
			map[string]interface{}{"indexed_at": "asc"},
			map[string]interface{}{"at_uri": "asc"},
		},
		"size":            size,
		"docvalue_fields": []interface{}{"embeddings.*"},
	}

	if len(after) > 0 {
		query["search_after"] = after
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

	decoder := json.NewDecoder(res.Body)
	decoder.UseNumber() // Keep numeric sort values exact when sending search_after.
	if err := decoder.Decode(&response); err != nil {
		return response, fmt.Errorf("failed to parse quality candidate response: %w", err)
	}

	logger.Metric("es.fetch_quality_candidates.took_ms", float64(response.Took))
	return response, nil
}
