package common

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func keysOf(m map[string]Float32Array) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func itoa(n int) string { return strconv.Itoa(n) }

func readAll(t *testing.T, r *http.Request) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return string(b)
}

func TestIndexNameForTime(t *testing.T) {
	ts := time.Date(2026, 7, 29, 14, 37, 0, 0, time.UTC)

	cases := []struct {
		base, period, want string
	}{
		{"posts", IndexPeriodWeek, "posts-2026-w31"},
		{"posts_quality", IndexPeriodWeek, "posts-quality-2026-w31"},
		{"posts_quality", IndexPeriodHour, "posts-quality-2026-07-29-14"},
		{"posts_quality", IndexPeriod10Min, "posts-quality-2026-07-29-14-30"},
		{"posts_quality", "nonsense", "posts-quality-2026-w31"},
	}

	for _, c := range cases {
		if got := IndexNameForTime(c.base, c.period, ts); got != c.want {
			t.Errorf("IndexNameForTime(%q, %q): got %q, want %q", c.base, c.period, got, c.want)
		}
	}
}

// CurrentIndexName must keep behaving exactly as before now that it delegates
// to IndexNameForTime — the posts/likes/tombstones write paths depend on it.
func TestCurrentIndexNameMatchesIndexNameForTime(t *testing.T) {
	for _, period := range []string{IndexPeriodWeek, IndexPeriodHour, IndexPeriod10Min} {
		now := time.Now().UTC()
		want := IndexNameForTime("posts", period, now)
		if got := CurrentIndexName("posts", period); got != want {
			t.Errorf("period %q: CurrentIndexName = %q, IndexNameForTime = %q", period, got, want)
		}
	}
}

func TestPostsCrossingQualityThreshold(t *testing.T) {
	t.Run("only posts that crossed upward are returned", func(t *testing.T) {
		results := []LikeCountResult{
			{AtURI: "at://a", LikeCount: 20, Increment: 1},  // 19 -> 20: crossed
			{AtURI: "at://b", LikeCount: 25, Increment: 1},  // 24 -> 25: already above
			{AtURI: "at://c", LikeCount: 19, Increment: 1},  // 18 -> 19: still below
			{AtURI: "at://d", LikeCount: 23, Increment: 5},  // 18 -> 23: crossed
			{AtURI: "at://e", LikeCount: 20, Increment: -1}, // 21 -> 20: downward, not a crossing
		}

		got := PostsCrossingQualityThreshold(results, 20)

		want := map[string]bool{"at://a": true, "at://d": true}
		if len(got) != len(want) {
			t.Fatalf("got %v, want keys %v", got, want)
		}
		for _, uri := range got {
			if !want[uri] {
				t.Errorf("unexpected at_uri %q in %v", uri, got)
			}
		}
	})

	t.Run("exactly-at-threshold on first like counts as crossing", func(t *testing.T) {
		got := PostsCrossingQualityThreshold(
			[]LikeCountResult{{AtURI: "at://x", LikeCount: 1, Increment: 1}}, 1)
		if len(got) != 1 || got[0] != "at://x" {
			t.Fatalf("got %v, want [at://x]", got)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		if got := PostsCrossingQualityThreshold(nil, 20); len(got) != 0 {
			t.Fatalf("got %v, want empty", got)
		}
	})
}

func TestQualityDocFromHit(t *testing.T) {
	hit := Hit{
		Source: PostData{
			AtURI:                  "at://did:plc:abc/app.bsky.feed.post/1",
			AuthorDID:              "did:plc:abc",
			Content:                "hello",
			CreatedAt:              "2026-07-29T10:00:00Z",
			IndexedAt:              "2026-07-29T10:01:00Z",
			LikeCount:              22,
			PostEmbeddingModelUUID: "uuid-1",
			ContainsVideo:          true,
			ContainsImages:         false,
			VideoCount:             1,
		},
		Fields: map[string]json.RawMessage{
			"embeddings.ge_post_embedding":          json.RawMessage(`[[0.1,0.2,0.3]]`),
			"embeddings.all_MiniLM_L12_v2":          json.RawMessage(`[[9,9,9]]`),
			"embeddings.google_embeddinggemma_300m": json.RawMessage(`[[8,8]]`),
		},
	}

	doc, ok := qualityDocFromHit(hit)
	if !ok {
		t.Fatal("expected hit to convert")
	}
	if doc.AtURI != hit.Source.AtURI || doc.AuthorDID != "did:plc:abc" {
		t.Errorf("identity fields not carried: %+v", doc)
	}
	if doc.LikeCount != 22 || doc.PostEmbeddingModelUUID != "uuid-1" {
		t.Errorf("ranking/filter fields not carried: %+v", doc)
	}
	if !doc.ContainsVideo || doc.VideoCount != 1 {
		t.Errorf("media filter fields not carried: %+v", doc)
	}

	// Only ge_post_embedding belongs in the quality index: it is the only vector
	// two-tower kNN searches, and the api hydrates L12 separately from
	// posts_recent. Copying the others would multiply the index's footprint,
	// which defeats the point of a lean, cache-resident corpus.
	if len(doc.Embeddings) != 1 {
		t.Fatalf("expected exactly one embedding family, got %v", keysOf(doc.Embeddings))
	}
	if len(doc.Embeddings[GEPostEmbeddingField]) != 3 {
		t.Errorf("ge_post_embedding not carried: %v", doc.Embeddings)
	}
}

func TestQualityDocFromHit_SkipsPostsWithoutSearchVector(t *testing.T) {
	// A post with no ge_post_embedding can never be returned by two-tower kNN,
	// so copying it would only add dead weight to the index.
	hit := Hit{
		Source: PostData{AtURI: "at://a", AuthorDID: "did:plc:a", CreatedAt: "2026-07-29T10:00:00Z"},
		Fields: map[string]json.RawMessage{
			"embeddings.all_MiniLM_L12_v2": json.RawMessage(`[[1,2,3]]`),
		},
	}
	if _, ok := qualityDocFromHit(hit); ok {
		t.Error("expected hit without ge_post_embedding to be skipped")
	}
}

func TestQualityDocFromHit_SkipsPostsMissingIdentity(t *testing.T) {
	// _routing is required on the quality template, so a doc without an author
	// DID would be rejected by ES at write time.
	hit := Hit{
		Source: PostData{AtURI: "at://a", CreatedAt: "2026-07-29T10:00:00Z"},
		Fields: map[string]json.RawMessage{
			"embeddings.ge_post_embedding": json.RawMessage(`[[1,2,3]]`),
		},
	}
	if _, ok := qualityDocFromHit(hit); ok {
		t.Error("expected hit without author_did to be skipped")
	}
}

func TestQualityIndexForPost(t *testing.T) {
	now := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)

	t.Run("buckets by the post's created_at, not by now", func(t *testing.T) {
		// A post created last week that only now crosses the threshold must land
		// in last week's quality index. Bucketing by now would let the same
		// at_uri appear in two period indices if it crossed twice, which would
		// surface as duplicate kNN candidates.
		got, ok := qualityIndexForPost("2026-07-22T12:00:00Z", IndexPeriodWeek, now, 14*24*time.Hour)
		if !ok {
			t.Fatal("expected post within retention to be accepted")
		}
		if got != "posts-quality-2026-w30" {
			t.Errorf("got %q, want posts-quality-2026-w30", got)
		}
	})

	t.Run("rejects posts older than the retention window", func(t *testing.T) {
		// ILM would delete such an index almost immediately; creating it is waste.
		if _, ok := qualityIndexForPost("2026-06-01T12:00:00Z", IndexPeriodWeek, now, 14*24*time.Hour); ok {
			t.Error("expected post older than retention to be rejected")
		}
	})

	t.Run("rejects unparseable created_at", func(t *testing.T) {
		if _, ok := qualityIndexForPost("not-a-date", IndexPeriodWeek, now, 14*24*time.Hour); ok {
			t.Error("expected unparseable created_at to be rejected")
		}
	})
}

func TestFetchPostsByAtURIs_RequestsEmbeddingsViaDocvalueFields(t *testing.T) {
	var capturedBody map[string]interface{}

	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer srv.Close()

	logger := NewLogger(false)
	_, err := FetchPostsByAtURIs(t.Context(), client, logger, "posts", []string{"at://a", "at://b"})
	if err != nil {
		t.Fatalf("FetchPostsByAtURIs returned error: %v", err)
	}

	// Same trap as FetchPosts: the posts template excludes "embeddings" from
	// _source, and "fields" silently returns nothing for dense_vector in that
	// case (greenearth-social/api#325). Without docvalue_fields the promoted
	// docs would carry no search vector at all.
	fields, ok := capturedBody["docvalue_fields"].([]interface{})
	if !ok || len(fields) == 0 || fields[0] != "embeddings.*" {
		t.Fatalf("expected docvalue_fields=[\"embeddings.*\"], got: %v", capturedBody)
	}
	if _, ok := capturedBody["fields"]; ok {
		t.Errorf("expected no \"fields\" param, got: %v", capturedBody)
	}
}

// PromoteQualityPosts is the ingest half of greenearth-social/ingex#442: it
// copies posts into the lean quality corpus the moment they cross the like
// threshold, so two-tower kNN can search an index where that filter is
// non-selective and Lucene keeps using the HNSW graph.
func TestPromoteQualityPosts(t *testing.T) {
	newSearchResponse := func(hits ...string) string {
		return `{"took":1,"hits":{"total":{"value":` +
			itoa(len(hits)) + `,"relation":"eq"},"hits":[` + strings.Join(hits, ",") + `]}}`
	}
	hitJSON := func(atURI, did, createdAt string) string {
		return `{"_index":"posts-2026-w30","_id":"` + atURI + `","_source":{"at_uri":"` + atURI +
			`","author_did":"` + did + `","created_at":"` + createdAt +
			`","like_count":20,"ge_post_embedding_model_uuid":"uuid-1"},` +
			`"fields":{"embeddings.ge_post_embedding":[[0.1,0.2]]}}`
	}

	t.Run("promotes crossings into the index for their created_at period", func(t *testing.T) {
		var searchCalls, bulkBodies []string

		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := readAll(t, r)
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(200)
			if strings.Contains(r.URL.Path, "_bulk") {
				bulkBodies = append(bulkBodies, body)
				_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[]}`))
				return
			}
			searchCalls = append(searchCalls, body)
			// 2026-07-22 is ISO week 30; "now" below is week 31.
			_, _ = w.Write([]byte(newSearchResponse(
				hitJSON("at://did:plc:a/app.bsky.feed.post/1", "did:plc:a", "2026-07-22T10:00:00Z"),
			)))
		}))
		defer srv.Close()

		now := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
		cfg := QualityPromotionConfig{
			SourceIndex:  "posts",
			Threshold:    20,
			IndexPeriod:  IndexPeriodWeek,
			RetentionAge: 14 * 24 * time.Hour,
			Now:          now,
		}
		results := []LikeCountResult{{AtURI: "at://did:plc:a/app.bsky.feed.post/1", LikeCount: 20, Increment: 1}}

		n, err := PromoteQualityPosts(t.Context(), client, NewLogger(false), cfg, results, false)
		if err != nil {
			t.Fatalf("PromoteQualityPosts: %v", err)
		}
		if n != 1 {
			t.Errorf("promoted %d docs, want 1", n)
		}
		if len(searchCalls) != 1 {
			t.Fatalf("expected exactly 1 lookup, got %d", len(searchCalls))
		}
		if len(bulkBodies) != 1 {
			t.Fatalf("expected exactly 1 bulk write, got %d", len(bulkBodies))
		}
		// created_at is in week 30, so the doc belongs to week 30's index even
		// though "now" is week 31.
		if !strings.Contains(bulkBodies[0], `"_index":"posts-quality-2026-w30"`) {
			t.Errorf("expected write to posts-quality-2026-w30, got: %s", bulkBodies[0])
		}
		if !strings.Contains(bulkBodies[0], `"routing":"did:plc:a"`) {
			t.Errorf("expected author_did routing (required by the template), got: %s", bulkBodies[0])
		}
		if !strings.Contains(bulkBodies[0], `"ge_post_embedding"`) {
			t.Errorf("expected the search vector in the promoted doc, got: %s", bulkBodies[0])
		}
	})

	t.Run("no crossings means no ES traffic at all", func(t *testing.T) {
		called := false
		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(200)
		}))
		defer srv.Close()

		cfg := QualityPromotionConfig{SourceIndex: "posts", Threshold: 20, IndexPeriod: IndexPeriodWeek,
			RetentionAge: 14 * 24 * time.Hour, Now: time.Now().UTC()}
		// Every post here is already above the threshold, so none of them crossed.
		results := []LikeCountResult{{AtURI: "at://a", LikeCount: 50, Increment: 1}}

		n, err := PromoteQualityPosts(t.Context(), client, NewLogger(false), cfg, results, false)
		if err != nil || n != 0 {
			t.Fatalf("got (%d, %v), want (0, nil)", n, err)
		}
		if called {
			t.Error("expected no ES request when nothing crossed the threshold")
		}
	})

	t.Run("dry run performs no writes", func(t *testing.T) {
		var sawBulk bool
		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "_bulk") {
				sawBulk = true
			}
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[],"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
		}))
		defer srv.Close()

		cfg := QualityPromotionConfig{SourceIndex: "posts", Threshold: 20, IndexPeriod: IndexPeriodWeek,
			RetentionAge: 14 * 24 * time.Hour, Now: time.Now().UTC()}
		results := []LikeCountResult{{AtURI: "at://a", LikeCount: 20, Increment: 1}}

		if _, err := PromoteQualityPosts(t.Context(), client, NewLogger(false), cfg, results, true); err != nil {
			t.Fatalf("PromoteQualityPosts: %v", err)
		}
		if sawBulk {
			t.Error("dry run must not issue bulk writes")
		}
	})
}

func TestBulkUpdateLikeCountsWithResults_ParsesUpdatedCounts(t *testing.T) {
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"took":3,"errors":false,"items":[
			{"update":{"_id":"at://did:plc:a/app.bsky.feed.post/1","status":200,"get":{"_source":{"like_count":20}}}},
			{"update":{"_id":"at://did:plc:b/app.bsky.feed.post/2","status":404,"error":{"type":"document_missing_exception","reason":"missing"}}}
		]}`))
	}))
	defer srv.Close()

	updates := []LikeCountUpdate{
		{SubjectURI: "at://did:plc:a/app.bsky.feed.post/1", Increment: 1},
		{SubjectURI: "at://did:plc:b/app.bsky.feed.post/2", Increment: 1},
	}

	results, err := BulkUpdateLikeCountsWithResults(t.Context(), client, "posts", updates, false, NewLogger(false))
	if err != nil {
		t.Fatalf("BulkUpdateLikeCountsWithResults: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (the 404 has no updated count), got %d: %+v", len(results), results)
	}
	if results[0].AtURI != "at://did:plc:a/app.bsky.feed.post/1" || results[0].LikeCount != 20 {
		t.Errorf("unexpected result: %+v", results[0])
	}
	if results[0].Increment != 1 {
		t.Errorf("increment must be carried through so crossings can be detected: %+v", results[0])
	}
}

// The quality corpus is what two_tower candidates come back from, so if it
// does not carry the perspective fields every two_tower candidate arrives at
// the api uncached and is scored live — defeating the point of scoring at
// ingest (api#368). The struct doc records this as an invariant; this test is
// what keeps it true.
func TestQualityDocFromHitCarriesPerspectiveFields(t *testing.T) {
	score := 0.42
	hit := Hit{
		Source: PostData{
			AtURI:                    "at://did:plc:a/app.bsky.feed.post/1",
			AuthorDID:                "did:plc:a",
			CreatedAt:                "2026-07-22T10:00:00Z",
			CombinedPerspectiveScore: &score,
			PerspectiveScoredAt:      "2026-07-22T10:05:00Z",
			// Present on the post, deliberately not copied: the corpus is
			// lean, the api never reads the raw attributes, and training
			// reads posts rather than this index.
			PerspectiveScores: map[string]float64{"toxicity": 0.1},
		},
		Fields: map[string]json.RawMessage{
			"embeddings.ge_post_embedding": json.RawMessage(`[[0.1,0.2]]`),
		},
	}

	doc, ok := qualityDocFromHit(hit)
	if !ok {
		t.Fatal("qualityDocFromHit returned not-ok for a complete hit")
	}
	if doc.CombinedPerspectiveScore == nil || *doc.CombinedPerspectiveScore != 0.42 {
		t.Errorf("combined score = %v, want 0.42", doc.CombinedPerspectiveScore)
	}
	if doc.PerspectiveScoredAt != "2026-07-22T10:05:00Z" {
		t.Errorf("scored_at = %q", doc.PerspectiveScoredAt)
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "perspective_scores") {
		t.Errorf("the lean corpus must not carry raw attribute scores: %s", encoded)
	}
}

// An unscored post still belongs in the corpus; it just arrives at the api
// uncached like it does today.
func TestQualityDocFromHitOmitsAbsentPerspectiveFields(t *testing.T) {
	hit := Hit{
		Source: PostData{
			AtURI:     "at://did:plc:a/app.bsky.feed.post/1",
			AuthorDID: "did:plc:a",
			CreatedAt: "2026-07-22T10:00:00Z",
		},
		Fields: map[string]json.RawMessage{
			"embeddings.ge_post_embedding": json.RawMessage(`[[0.1,0.2]]`),
		},
	}

	doc, ok := qualityDocFromHit(hit)
	if !ok {
		t.Fatal("an unscored post must still be promotable")
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "perspective") {
		t.Errorf("absent perspective fields must be omitted entirely: %s", encoded)
	}
}
