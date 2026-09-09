package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// GEPostEmbeddingField is the only embedding family the quality corpus carries:
// it is the vector two-tower kNN searches. The api hydrates MiniLM L12
// separately from posts_recent (see routers/xrpc.py), so copying the other
// families here would multiply the index footprint for no read.
const GEPostEmbeddingField = "ge_post_embedding"

// QualityPostDoc is the lean document written to the quality corpus. It carries
// the fields two-tower kNN filters on (created_at, ge_post_embedding_model_uuid,
// contains_video, like_count) plus the fields the api reads back off a kNN hit
// (CANDIDATE_SOURCE_FIELDS in api's lib/candidates/utils.py) — and nothing else.
//
// like_count is a point-in-time snapshot taken when the post crossed the
// threshold, and is deliberately not maintained afterwards. It is never a
// ranking input: both rankers refetch like_count from posts_recent
// (api's lib/rankers/{heavy_ranker,two_tower}.py). As a filter it is safe to
// leave stale, because a post's like count only grows in the common case, so a
// stale value at or above the threshold implies the live value is too.
type QualityPostDoc struct {
	AtURI                  string                  `json:"at_uri"`
	AuthorDID              string                  `json:"author_did"`
	Content                string                  `json:"content"`
	CreatedAt              string                  `json:"created_at"`
	Embeddings             map[string]Float32Array `json:"embeddings,omitempty"`
	PostEmbeddingModelUUID string                  `json:"ge_post_embedding_model_uuid"`
	IndexedAt              string                  `json:"indexed_at"`
	LikeCount              int                     `json:"like_count"`
	ContainsImages         bool                    `json:"contains_images"`
	ContainsVideo          bool                    `json:"contains_video"`
	ImageCount             int                     `json:"image_count"`
	VideoCount             int                     `json:"video_count"`
	ExternalEmbed          *ExternalEmbed          `json:"external_embed"`

	// Perspective fields, carried because the api's perspective ranker reads
	// them off a kNN hit like any other candidate field (api#368). The raw
	// per-attribute perspective_scores map is deliberately *not* copied: the
	// api only ever reads the combined score, and the raw attributes exist for
	// training, which reads posts, not this corpus.
	CombinedPerspectiveScore *float64 `json:"combined_perspective_score,omitempty"`
	PerspectiveScoredAt      string   `json:"perspective_scored_at,omitempty"`
}

func (d QualityPostDoc) esAtURI() string     { return d.AtURI }
func (d QualityPostDoc) esAuthorDID() string { return d.AuthorDID }

// QualityPromotionConfig parameterises a promotion pass.
type QualityPromotionConfig struct {
	// SourceIndex is read to recover full documents (including the search
	// vector) for posts that crossed the threshold.
	SourceIndex string
	// Threshold is the like count at which a post joins the quality corpus.
	Threshold int
	// IndexPeriod controls the period bucket of the destination index.
	IndexPeriod string
	// RetentionAge mirrors the quality index's ILM delete age. Posts older than
	// this are not promoted: their index would be deleted almost immediately.
	RetentionAge time.Duration
	// Now is the reference time for the retention check. Zero means time.Now().
	Now time.Time
}

// PostsCrossingQualityThreshold returns the at_uris that moved from below the
// threshold to at-or-above it in this batch.
//
// Testing for a crossing rather than for "is at or above" is what keeps this
// cheap: a popular post receives likes indefinitely, and promoting on every one
// of them would mean a document fetch and a rewrite per like. A post crosses
// once, so steady-state promotion traffic is a small fraction of like traffic.
func PostsCrossingQualityThreshold(results []LikeCountResult, threshold int) []string {
	var crossed []string
	for _, r := range results {
		if r.Increment <= 0 {
			continue
		}
		if r.LikeCount >= threshold && r.LikeCount-r.Increment < threshold {
			crossed = append(crossed, r.AtURI)
		}
	}
	return crossed
}

// qualityIndexForPost returns the destination index for a post, bucketed by the
// post's own created_at rather than by the current time.
//
// Bucketing by created_at makes the mapping at_uri -> index stable: a post that
// crosses the threshold, is unliked below it, and crosses again lands in the
// same index both times and is overwritten in place. Bucketing by now would let
// the same at_uri exist in two period indices at once, which the alias would
// surface as duplicate kNN candidates.
func qualityIndexForPost(createdAt, period string, now time.Time, retention time.Duration) (string, bool) {
	ts, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return "", false
	}
	if retention > 0 && now.Sub(ts) > retention {
		return "", false
	}
	return IndexNameForTime("posts_quality", period, ts.UTC()), true
}

// qualityDocFromHit projects a posts hit onto the lean quality document.
// Posts without a search vector or without the identity fields the template
// requires are skipped.
func qualityDocFromHit(hit Hit) (QualityPostDoc, bool) {
	src := hit.Source
	if src.AtURI == "" || src.AuthorDID == "" {
		return QualityPostDoc{}, false
	}

	vec := embeddingsFromHit(hit)[GEPostEmbeddingField]
	if len(vec) == 0 {
		return QualityPostDoc{}, false
	}

	return QualityPostDoc{
		AtURI:                  src.AtURI,
		AuthorDID:              src.AuthorDID,
		Content:                src.Content,
		CreatedAt:              src.CreatedAt,
		Embeddings:             map[string]Float32Array{GEPostEmbeddingField: Float32Array(vec)},
		PostEmbeddingModelUUID: src.PostEmbeddingModelUUID,
		IndexedAt:              src.IndexedAt,
		LikeCount:              src.LikeCount,
		ContainsImages:         src.ContainsImages,
		ContainsVideo:          src.ContainsVideo,
		ImageCount:             src.ImageCount,
		VideoCount:             src.VideoCount,
		ExternalEmbed:          src.ExternalEmbed,

		CombinedPerspectiveScore: src.CombinedPerspectiveScore,
		PerspectiveScoredAt:      src.PerspectiveScoredAt,
	}, true
}

// PromoteQualityPosts copies posts that just crossed the like threshold into
// the quality corpus, and returns how many documents were written.
//
// Errors are returned rather than swallowed, but callers on the ingest hot path
// should treat a failure as non-fatal: the quality corpus is a derived,
// rebuildable view (see scripts/backfill_quality_index.py), and a missed
// promotion costs one post's retrievability, not correctness of posts itself.
func PromoteQualityPosts(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *IngestLogger,
	cfg QualityPromotionConfig,
	results []LikeCountResult,
	dryRun bool,
) (int, error) {
	crossed := PostsCrossingQualityThreshold(results, cfg.Threshold)
	if len(crossed) == 0 {
		return 0, nil
	}

	now := cfg.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	logger.Metric("quality_posts.crossed_count", float64(len(crossed)))

	start := time.Now()
	resp, err := FetchPostsByAtURIs(ctx, client, logger, cfg.SourceIndex, crossed)
	if err != nil {
		return 0, fmt.Errorf("fetch posts crossing quality threshold: %w", err)
	}
	logger.Metric("es.promote_quality_posts.fetch_duration_ms", float64(time.Since(start).Milliseconds()))

	// Group by destination index: a batch can span period boundaries.
	byIndex := make(map[string][]QualityPostDoc)
	skipped := 0
	for _, hit := range resp.Hits.Hits {
		doc, ok := qualityDocFromHit(hit)
		if !ok {
			skipped++
			continue
		}
		index, ok := qualityIndexForPost(doc.CreatedAt, cfg.IndexPeriod, now, cfg.RetentionAge)
		if !ok {
			skipped++
			continue
		}
		byIndex[index] = append(byIndex[index], doc)
	}

	if skipped > 0 {
		logger.Debug("Skipped %d of %d threshold crossings (no search vector, missing identity, or outside retention)", skipped, len(crossed))
	}

	promoted := 0
	for index, docs := range byIndex {
		if err := BulkIndex(ctx, client, index, docs, dryRun, logger); err != nil {
			return promoted, fmt.Errorf("promote %d posts to %s: %w", len(docs), index, err)
		}
		promoted += len(docs)
	}

	if !dryRun && promoted > 0 {
		logger.Metric("quality_posts.promoted_count", float64(promoted))
	}
	return promoted, nil
}

// FetchPostsByAtURIs retrieves full post documents for the given at_uris.
//
// Embeddings come from docvalue_fields, not "fields" and not _source: the posts
// template excludes "embeddings" from _source, and on this ES version "fields"
// falls back to decompressing _source for dense_vector, so it silently returns
// nothing (greenearth-social/api#325). Getting this wrong would produce a
// quality index of documents with no search vector — an index that answers
// every kNN query with nothing.
func FetchPostsByAtURIs(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *IngestLogger,
	index string,
	atURIs []string,
) (SearchResponse, error) {
	var response SearchResponse
	if len(atURIs) == 0 {
		return response, nil
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{"at_uri": atURIs},
		},
		"size":            len(atURIs),
		"docvalue_fields": []interface{}{"embeddings.*"},
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
	logger.Metric("es.fetch_posts_by_at_uris.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return response, fmt.Errorf("post by-at-uri search request failed: %w", err)
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			logger.Error("Failed to close search response body: %v", cerr)
		}
	}()

	if res.IsError() {
		return response, fmt.Errorf("post by-at-uri search returned error: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("failed to parse post by-at-uri search response: %w", err)
	}

	logger.Metric("es.fetch_posts_by_at_uris.took_ms", float64(response.Took))
	return response, nil
}
