package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// PerspectiveUpdate is one post's scoring outcome, ready to write.
//
// Scores and CombinedScore are nil for a post the API declined to score;
// ScoredAt is still set, which is what marks it permanently unscorable rather
// than merely not-yet-scored.
type PerspectiveUpdate struct {
	// Index is the concrete index holding the document, as the scan reported
	// it in _index — never an alias. A bulk update addressed to an alias is
	// routed to that alias's *write* index, so with posts_recent spanning a
	// weekly index per retention period, every document outside the current
	// period would come back document_missing. Empty is a programming error.
	Index         string
	AtURI         string
	Scores        map[string]float64
	CombinedScore *float64
	ScoredAt      string
}

// BulkUpdatePerspectiveScores writes perspective fields onto existing posts.
//
// This is a partial `update` rather than a re-index for a reason worth keeping:
// the documents it targets already carry their embeddings, and rewriting a
// whole document to add three fields would mean reading those vectors back out
// and re-sending them — the same trap BackfillQualityPosts documents, and a
// large amount of traffic for a small change. A partial update leaves every
// other field, embeddings included, untouched.
//
// Routing follows BulkUpdateLikeCounts: the posts mapping requires routing, and
// the author DID comes out of the AT-URI, so no extra read is needed. Posts
// whose URI yields no DID are skipped rather than failing the batch.
//
// Each update names its own index rather than the batch sharing one. The bulk
// API takes _index per action, so one request still covers every period index
// a page spans — and there is deliberately no batch-level index to fall back
// on, because the value that looks most natural to pass ("posts_recent", the
// index the scan was given) is an alias, and that is exactly the bug: the
// update lands on the alias's write index, where the document is not.
//
// Returns the number of documents successfully updated.
func BulkUpdatePerspectiveScores(ctx context.Context, client *elasticsearch.Client, updates []PerspectiveUpdate, dryRun bool, logger *IngestLogger) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk update of %d post perspective scores", len(updates))
		return 0, nil
	}

	var buf bytes.Buffer
	queued := 0
	skippedNoRouting := 0
	skippedNoIndex := 0

	for _, update := range updates {
		authorDID := ExtractDIDFromATURI(update.AtURI)
		if authorDID == "" {
			skippedNoRouting++
			continue
		}
		if update.Index == "" {
			skippedNoIndex++
			continue
		}

		meta := map[string]interface{}{
			"update": map[string]interface{}{
				"_index":  update.Index,
				"_id":     update.AtURI,
				"routing": authorDID,
			},
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal update metadata: %w", err)
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		doc := map[string]interface{}{"perspective_scored_at": update.ScoredAt}
		if update.CombinedScore != nil {
			doc["combined_perspective_score"] = *update.CombinedScore
		}
		if len(update.Scores) > 0 {
			doc["perspective_scores"] = update.Scores
		}

		updateJSON, err := json.Marshal(map[string]interface{}{"doc": doc})
		if err != nil {
			return 0, fmt.Errorf("failed to marshal update body: %w", err)
		}
		buf.Write(updateJSON)
		buf.WriteByte('\n')
		queued++
	}

	// A malformed AT-URI is bad data rather than a fault here, so it stays at
	// Debug as the like-count path logs the same skip. Counted anyway: a post
	// whose URI will not yield a DID can never be scored, and a rate that is
	// anything but flat-zero says something upstream has changed.
	if skippedNoRouting > 0 {
		logger.Metric("es.update_perspective_scores.skipped_no_routing_count", float64(skippedNoRouting))
		logger.Debug("Skipped %d perspective updates with unparseable AT-URIs", skippedNoRouting)
	}
	// Unlike an unparseable AT-URI, this is a caller bug rather than bad data.
	if skippedNoIndex > 0 {
		logger.Error("Skipped %d perspective updates with no index set; the scan must "+
			"carry each hit's _index through to the update", skippedNoIndex)
	}
	if queued == 0 {
		return 0, nil
	}

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.update_perspective_scores.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return 0, fmt.Errorf("bulk update request failed: %w", err)
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			logger.Error("Failed to close response body: %v", cerr)
		}
	}()

	if res.IsError() {
		return 0, fmt.Errorf("bulk update request returned error: %s", res.String())
	}

	var bulkResponse struct {
		Took   int  `json:"took"`
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return 0, fmt.Errorf("failed to parse bulk update response: %w", err)
	}

	logger.Metric("es.update_perspective_scores.took_ms", float64(bulkResponse.Took))

	updated := 0
	missing := 0
	firstError := ""
	for _, item := range bulkResponse.Items {
		for _, details := range item {
			if details.Error != nil {
				// A 404 is routine *per document*: posts age out of the index
				// between the scan and the write. Anything else is worth
				// surfacing.
				if details.Status == 404 {
					missing++
				} else if firstError == "" {
					firstError = fmt.Sprintf("%s: %s", details.Error.Type, details.Error.Reason)
				}
				continue
			}
			updated++
		}
	}

	if firstError != "" {
		logger.Error("Some perspective updates failed (first error: %s)", firstError)
	}

	// Routine one at a time, a symptom in bulk: a whole batch coming back
	// missing is what a misrouted write looks like, and suppressing 404s as
	// expected is what let exactly that go unnoticed. Counted so the ratio to
	// updated_count is visible rather than inferred from a low update count.
	if missing > 0 {
		logger.Metric("es.update_perspective_scores.missing_count", float64(missing))
		if missing == queued {
			logger.Error("All %d perspective updates in this batch reported document_missing; "+
				"if this repeats, the writes are going to the wrong index", missing)
		} else {
			logger.Debug("%d of %d perspective updates reported document_missing (aged out)", missing, queued)
		}
	}

	logger.Metric("es.update_perspective_scores.updated_count", float64(updated))
	return updated, nil
}

// UnscoredPost is a post the backfill needs to score: the text to send, and
// the URI and concrete index to write the result back to.
type UnscoredPost struct {
	AtURI   string
	Content string
	// Index is the hit's _index. The scan is normally given the posts_recent
	// alias, which spans one index per retention period, so the index a
	// document lives in has to travel with it — see PerspectiveUpdate.Index.
	Index string
}

// PostScanCursor carries a page's paging state. HitCount is the number of hits
// Elasticsearch returned, which is not always len(posts) — hits missing an
// at_uri are unusable and dropped — and it is the hit count, not the usable
// count, that tells the caller whether a further page exists.
//
// All three sort values are carried because the scan sorts on all three: see
// FetchUnscoredPosts on why at_uri is not optional.
type PostScanCursor struct {
	CreatedAt string
	IndexedAt string
	AtURI     string
	HitCount  int
}

// FetchUnscoredPosts pages through posts that have never been submitted to the
// Perspective API, oldest first.
//
// "Never submitted" is the absence of perspective_scored_at, not the absence of
// a score: posts the API declined to score carry the timestamp with no score,
// and re-submitting them would burn quota to be declined again.
//
// Paging is search_after over (created_at, indexed_at, at_uri). The at_uri is
// the part that is easy to leave out and expensive to omit: search_after
// resumes strictly *after* the given sort key, so if a page ends part-way
// through a group of documents sharing a sort key, every remaining member of
// that group is skipped. created_at has second granularity and indexed_at is
// stamped per ingest batch, so those ties are common — measured at ~2% of
// posts silently never scored, on a real index, before at_uri was added.
// at_uri is unique, which makes the sort a total order and the paging exact.
//
// A run that dies partway can resume from the last reported cursor. Note the
// scan is not stable against its own writes — a scored post drops out of the
// result set — which is why the caller pages forward rather than repeatedly
// re-querying from the start.
func FetchUnscoredPosts(
	ctx context.Context,
	client *elasticsearch.Client,
	logger *IngestLogger,
	index string,
	startTime string,
	after PostScanCursor,
	size int,
) ([]UnscoredPost, PostScanCursor, error) {
	if size <= 0 {
		size = 500
	}

	filters := []interface{}{
		map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": map[string]interface{}{
					"exists": map[string]interface{}{"field": "perspective_scored_at"},
				},
			},
		},
	}
	if startTime != "" {
		filters = append(filters, map[string]interface{}{
			"range": map[string]interface{}{
				"created_at": map[string]interface{}{"gte": startTime},
			},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": filters},
		},
		"size":    size,
		"_source": []string{"at_uri", "content", "created_at", "indexed_at"},
		"sort": []interface{}{
			map[string]interface{}{"created_at": "asc"},
			map[string]interface{}{"indexed_at": "asc"},
			map[string]interface{}{"at_uri": "asc"},
		},
	}
	if after.CreatedAt != "" && after.IndexedAt != "" && after.AtURI != "" {
		query["search_after"] = []interface{}{after.CreatedAt, after.IndexedAt, after.AtURI}
	}

	var cursor PostScanCursor

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, cursor, fmt.Errorf("failed to marshal query: %w", err)
	}

	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	logger.Metric("es.fetch_unscored_posts.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return nil, cursor, fmt.Errorf("unscored-post search request failed: %w", err)
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			logger.Error("Failed to close search response body: %v", cerr)
		}
	}()

	if res.IsError() {
		return nil, cursor, fmt.Errorf("unscored-post search returned error: %s", res.String())
	}

	var response struct {
		Hits struct {
			Hits []struct {
				Index  string `json:"_index"`
				Source struct {
					AtURI     string `json:"at_uri"`
					Content   string `json:"content"`
					CreatedAt string `json:"created_at"`
					IndexedAt string `json:"indexed_at"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, cursor, fmt.Errorf("failed to parse unscored-post search response: %w", err)
	}

	cursor.HitCount = len(response.Hits.Hits)
	posts := make([]UnscoredPost, 0, cursor.HitCount)
	for _, hit := range response.Hits.Hits {
		cursor.CreatedAt = hit.Source.CreatedAt
		cursor.IndexedAt = hit.Source.IndexedAt
		cursor.AtURI = hit.Source.AtURI
		if hit.Source.AtURI == "" {
			continue
		}
		posts = append(posts, UnscoredPost{
			AtURI:   hit.Source.AtURI,
			Content: hit.Source.Content,
			Index:   hit.Index,
		})
	}

	return posts, cursor, nil
}
