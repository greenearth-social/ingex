package common

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func float64Ptr(v float64) *float64 { return &v }

func esOK(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(body))
}

// The write is a partial update, not a re-index. A re-index would have to
// carry the document's embeddings along with it — read back out of doc values
// and re-sent — to avoid destroying them, which is a large amount of traffic to
// add three fields. See the note on BackfillQualityPosts for the version of
// this trap that already bit us.
func TestBulkUpdatePerspectiveScoresUsesPartialUpdate(t *testing.T) {
	var bulkBody string
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bulkBody = readAll(t, r)
		esOK(w, `{"took":1,"errors":false,"items":[{"update":{"_id":"at://did:plc:a/app.bsky.feed.post/1","status":200}}]}`)
	}))
	defer srv.Close()

	updates := []PerspectiveUpdate{{
		AtURI:         "at://did:plc:a/app.bsky.feed.post/1",
		Scores:        map[string]float64{"toxicity": 0.25},
		CombinedScore: float64Ptr(0.75),
		ScoredAt:      "2026-08-28T00:00:00Z",
	}}

	updated, err := BulkUpdatePerspectiveScores(t.Context(), client, "posts_recent", updates, false, NewLogger(false))
	if err != nil {
		t.Fatalf("BulkUpdatePerspectiveScores: %v", err)
	}
	if updated != 1 {
		t.Errorf("updated = %d, want 1", updated)
	}

	lines := strings.Split(strings.TrimSpace(bulkBody), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected an action line and a doc line, got %d lines: %q", len(lines), bulkBody)
	}

	var action map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatalf("action line is not JSON: %v", err)
	}
	meta, ok := action["update"]
	if !ok {
		t.Fatalf("action is %v, want an update (an index action would replace the document)", action)
	}
	if meta["_index"] != "posts_recent" || meta["_id"] != "at://did:plc:a/app.bsky.feed.post/1" {
		t.Errorf("action metadata = %v", meta)
	}
	// The posts mapping requires routing; without it the update lands on the
	// wrong shard and silently does nothing.
	if meta["routing"] != "did:plc:a" {
		t.Errorf("routing = %v, want the author DID", meta["routing"])
	}

	var doc struct {
		Doc map[string]interface{} `json:"doc"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &doc); err != nil {
		t.Fatalf("doc line is not JSON: %v", err)
	}
	if len(doc.Doc) != 3 {
		t.Errorf("partial doc has %d fields (%v), want only the three perspective fields", len(doc.Doc), doc.Doc)
	}
	if doc.Doc["combined_perspective_score"] != 0.75 {
		t.Errorf("combined score = %v", doc.Doc["combined_perspective_score"])
	}
	if doc.Doc["perspective_scored_at"] != "2026-08-28T00:00:00Z" {
		t.Errorf("scored_at = %v", doc.Doc["perspective_scored_at"])
	}
}

// An unscorable post is written as a bare timestamp. Writing a null score
// instead would be indistinguishable from a real one once it reached the api.
func TestBulkUpdatePerspectiveScoresWritesStampOnlyForUnscorable(t *testing.T) {
	var bulkBody string
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bulkBody = readAll(t, r)
		esOK(w, `{"took":1,"errors":false,"items":[{"update":{"_id":"x","status":200}}]}`)
	}))
	defer srv.Close()

	updates := []PerspectiveUpdate{{
		AtURI:    "at://did:plc:a/app.bsky.feed.post/1",
		ScoredAt: "2026-08-28T00:00:00Z",
	}}
	if _, err := BulkUpdatePerspectiveScores(t.Context(), client, "posts_recent", updates, false, NewLogger(false)); err != nil {
		t.Fatalf("BulkUpdatePerspectiveScores: %v", err)
	}

	docLine := strings.Split(strings.TrimSpace(bulkBody), "\n")[1]
	if strings.Contains(docLine, "combined_perspective_score") {
		t.Errorf("an unscorable post must carry no score field: %s", docLine)
	}
	if strings.Contains(docLine, "perspective_scores") {
		t.Errorf("an unscorable post must carry no attribute scores: %s", docLine)
	}
	if !strings.Contains(docLine, "perspective_scored_at") {
		t.Errorf("an unscorable post must still be stamped: %s", docLine)
	}
}

// 0.0 is maximally toxic, not "no score". Dropping it would quietly turn the
// worst posts into unscored ones.
func TestBulkUpdatePerspectiveScoresWritesZeroScore(t *testing.T) {
	var bulkBody string
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bulkBody = readAll(t, r)
		esOK(w, `{"took":1,"errors":false,"items":[{"update":{"_id":"x","status":200}}]}`)
	}))
	defer srv.Close()

	updates := []PerspectiveUpdate{{
		AtURI:         "at://did:plc:a/app.bsky.feed.post/1",
		CombinedScore: float64Ptr(0),
		ScoredAt:      "2026-08-28T00:00:00Z",
	}}
	if _, err := BulkUpdatePerspectiveScores(t.Context(), client, "posts_recent", updates, false, NewLogger(false)); err != nil {
		t.Fatalf("BulkUpdatePerspectiveScores: %v", err)
	}
	if !strings.Contains(bulkBody, `"combined_perspective_score":0`) {
		t.Errorf("a 0.0 score was dropped from the update: %s", bulkBody)
	}
}

func TestBulkUpdatePerspectiveScoresSkipsUnroutablePosts(t *testing.T) {
	called := false
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		esOK(w, `{"took":1,"errors":false,"items":[]}`)
	}))
	defer srv.Close()

	updates := []PerspectiveUpdate{{AtURI: "not-an-at-uri", ScoredAt: "2026-08-28T00:00:00Z"}}
	updated, err := BulkUpdatePerspectiveScores(t.Context(), client, "posts_recent", updates, false, NewLogger(false))
	if err != nil {
		t.Fatalf("BulkUpdatePerspectiveScores: %v", err)
	}
	if updated != 0 {
		t.Errorf("updated = %d, want 0", updated)
	}
	if called {
		t.Error("an empty batch should not reach Elasticsearch")
	}
}

// Posts age out of the index between the scan and the write, so a 404 is
// routine and must not be reported as a failure.
func TestBulkUpdatePerspectiveScoresTolerates404(t *testing.T) {
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		esOK(w, `{"took":1,"errors":true,"items":[
			{"update":{"_id":"a","status":404,"error":{"type":"document_missing_exception","reason":"gone"}}},
			{"update":{"_id":"b","status":200}}
		]}`)
	}))
	defer srv.Close()

	updates := []PerspectiveUpdate{
		{AtURI: "at://did:plc:a/app.bsky.feed.post/1", ScoredAt: "t"},
		{AtURI: "at://did:plc:b/app.bsky.feed.post/2", ScoredAt: "t"},
	}
	updated, err := BulkUpdatePerspectiveScores(t.Context(), client, "posts_recent", updates, false, NewLogger(false))
	if err != nil {
		t.Fatalf("a missing document must not fail the batch: %v", err)
	}
	if updated != 1 {
		t.Errorf("updated = %d, want 1", updated)
	}
}

func TestBulkUpdatePerspectiveScoresDryRunWritesNothing(t *testing.T) {
	updates := []PerspectiveUpdate{{AtURI: "at://did:plc:a/app.bsky.feed.post/1", ScoredAt: "t"}}
	updated, err := BulkUpdatePerspectiveScores(t.Context(), nil, "posts_recent", updates, true, NewLogger(false))
	if err != nil {
		t.Fatalf("dry run error: %v", err)
	}
	if updated != 0 {
		t.Errorf("updated = %d, want 0", updated)
	}
}

// The scan selects on the absence of perspective_scored_at, not the absence of
// a score. Selecting on the score would re-submit every non-English post on
// every run, spending quota to be refused again.
func TestFetchUnscoredPostsSelectsOnMissingStamp(t *testing.T) {
	var searchBody string
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		searchBody = readAll(t, r)
		esOK(w, `{"hits":{"hits":[
			{"_source":{"at_uri":"at://did:plc:a/app.bsky.feed.post/1","content":"hello","created_at":"2026-08-01T00:00:00Z","indexed_at":"2026-08-01T00:00:01Z"}}
		]}}`)
	}))
	defer srv.Close()

	posts, cursor, err := FetchUnscoredPosts(t.Context(), client, NewLogger(false), "posts_recent", "2026-07-01T00:00:00Z", PostScanCursor{}, 10)
	if err != nil {
		t.Fatalf("FetchUnscoredPosts: %v", err)
	}

	if !strings.Contains(searchBody, `"must_not"`) || !strings.Contains(searchBody, `"perspective_scored_at"`) {
		t.Errorf("query does not exclude already-attempted posts: %s", searchBody)
	}
	if strings.Contains(searchBody, "combined_perspective_score") {
		t.Errorf("query selects on the score rather than the stamp: %s", searchBody)
	}
	if strings.Contains(searchBody, `"search_after"`) {
		t.Error("a first page must not send search_after")
	}

	if len(posts) != 1 || posts[0].Content != "hello" {
		t.Fatalf("posts = %+v", posts)
	}
	if cursor.HitCount != 1 || cursor.CreatedAt != "2026-08-01T00:00:00Z" || cursor.IndexedAt != "2026-08-01T00:00:01Z" {
		t.Errorf("cursor = %+v", cursor)
	}
	// at_uri makes the sort a total order. Without it, search_after skips
	// every document sharing the last page's (created_at, indexed_at) — and
	// those ties are common, because created_at has second granularity and
	// indexed_at is stamped once per ingest batch.
	if !strings.Contains(searchBody, `{"at_uri":"asc"}`) {
		t.Errorf("scan sort has no unique tiebreaker: %s", searchBody)
	}
	if cursor.AtURI != "at://did:plc:a/app.bsky.feed.post/1" {
		t.Errorf("cursor carries no at_uri: %+v", cursor)
	}
}

func TestFetchUnscoredPostsPagesWithSearchAfter(t *testing.T) {
	var searchBody string
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		searchBody = readAll(t, r)
		esOK(w, `{"hits":{"hits":[]}}`)
	}))
	defer srv.Close()

	after := PostScanCursor{
		CreatedAt: "2026-08-01T00:00:00Z",
		IndexedAt: "2026-08-01T00:00:01Z",
		AtURI:     "at://did:plc:a/app.bsky.feed.post/1",
	}
	if _, _, err := FetchUnscoredPosts(t.Context(), client, NewLogger(false), "posts_recent", "", after, 10); err != nil {
		t.Fatalf("FetchUnscoredPosts: %v", err)
	}
	if !strings.Contains(searchBody, `"search_after":["2026-08-01T00:00:00Z","2026-08-01T00:00:01Z","at://did:plc:a/app.bsky.feed.post/1"]`) {
		t.Errorf("second page did not send the full cursor: %s", searchBody)
	}
}

// HitCount, not len(posts), decides whether another page exists — a hit with
// no at_uri is dropped from posts but still consumed a slot in the page.
func TestFetchUnscoredPostsCountsUnusableHits(t *testing.T) {
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		esOK(w, `{"hits":{"hits":[
			{"_source":{"at_uri":"","content":"x","created_at":"2026-08-01T00:00:00Z","indexed_at":"2026-08-01T00:00:01Z"}},
			{"_source":{"at_uri":"at://did:plc:a/app.bsky.feed.post/1","content":"y","created_at":"2026-08-01T00:00:02Z","indexed_at":"2026-08-01T00:00:03Z"}}
		]}}`)
	}))
	defer srv.Close()

	posts, cursor, err := FetchUnscoredPosts(t.Context(), client, NewLogger(false), "posts_recent", "", PostScanCursor{}, 10)
	if err != nil {
		t.Fatalf("FetchUnscoredPosts: %v", err)
	}
	if len(posts) != 1 {
		t.Errorf("got %d usable posts, want 1", len(posts))
	}
	if cursor.HitCount != 2 {
		t.Errorf("HitCount = %d, want 2 — it must count hits, not usable posts", cursor.HitCount)
	}
	// The cursor advances past the unusable hit too, or the next page repeats it.
	if cursor.CreatedAt != "2026-08-01T00:00:02Z" {
		t.Errorf("cursor did not advance to the last hit: %+v", cursor)
	}
}
