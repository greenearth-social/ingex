package common

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFetchQualityCandidates_QueryShape(t *testing.T) {
	var body map[string]interface{}

	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer srv.Close()

	_, err := FetchQualityCandidates(t.Context(), client, NewLogger(false),
		"posts_recent", 20, "2026-07-15T00:00:00Z", "", "", 500)
	if err != nil {
		t.Fatalf("FetchQualityCandidates: %v", err)
	}

	// The vector must come from docvalue_fields. _reindex and _source-based
	// copies both silently drop it, because the posts template excludes
	// "embeddings" from _source — that would build a quality index whose every
	// kNN query returns nothing.
	dv, ok := body["docvalue_fields"].([]interface{})
	if !ok || len(dv) == 0 || dv[0] != "embeddings.*" {
		t.Fatalf("expected docvalue_fields=[\"embeddings.*\"], got %v", body)
	}

	filters, ok := body["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"].([]interface{})
	if !ok {
		t.Fatalf("expected a bool filter query, got %v", body["query"])
	}
	var sawLike, sawCreated bool
	for _, f := range filters {
		rng, ok := f.(map[string]interface{})["range"].(map[string]interface{})
		if !ok {
			continue
		}
		if lc, ok := rng["like_count"].(map[string]interface{}); ok {
			sawLike = true
			if gte, ok := lc["gte"].(float64); !ok || gte != 20 {
				t.Errorf("like_count gte = %v, want 20", lc["gte"])
			}
		}
		if ca, ok := rng["created_at"].(map[string]interface{}); ok {
			sawCreated = true
			if ca["gte"] != "2026-07-15T00:00:00Z" {
				t.Errorf("created_at gte = %v", ca["gte"])
			}
		}
	}
	if !sawLike || !sawCreated {
		t.Errorf("expected both like_count and created_at filters, got %v", filters)
	}

	// Deterministic pagination: search_after needs a total sort order.
	sort, ok := body["sort"].([]interface{})
	if !ok || len(sort) != 2 {
		t.Fatalf("expected a two-key sort for search_after, got %v", body["sort"])
	}
}

func TestFetchQualityCandidates_PassesSearchAfterCursor(t *testing.T) {
	var body map[string]interface{}
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer srv.Close()

	_, err := FetchQualityCandidates(t.Context(), client, NewLogger(false),
		"posts_recent", 20, "2026-07-15T00:00:00Z", "2026-07-20T00:00:00Z", "2026-07-20T01:00:00Z", 500)
	if err != nil {
		t.Fatalf("FetchQualityCandidates: %v", err)
	}

	after, ok := body["search_after"].([]interface{})
	if !ok || len(after) != 2 || after[0] != "2026-07-20T00:00:00Z" {
		t.Fatalf("expected search_after cursor, got %v", body["search_after"])
	}
}

// BackfillQualityPosts seeds the quality corpus from posts_recent. It is the
// one-time counterpart to the jetstream promotion path: without it the corpus
// only ever contains posts that crossed the threshold after deploy.
func TestBackfillQualityPosts(t *testing.T) {
	hit := func(n, createdAt string) string {
		return `{"_id":"at://did:plc:a/app.bsky.feed.post/` + n + `","_source":{` +
			`"at_uri":"at://did:plc:a/app.bsky.feed.post/` + n + `","author_did":"did:plc:a",` +
			`"created_at":"` + createdAt + `","indexed_at":"` + createdAt + `","like_count":25,` +
			`"ge_post_embedding_model_uuid":"uuid-1"},` +
			`"sort":["` + createdAt + `","` + createdAt + `"],` +
			`"fields":{"embeddings.ge_post_embedding":[[0.1,0.2]]}}`
	}

	t.Run("pages until exhausted and writes each period bucket", func(t *testing.T) {
		var searches int
		var bulkBodies []string

		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(200)
			if strings.Contains(r.URL.Path, "_bulk") {
				b := readAll(t, r)
				bulkBodies = append(bulkBodies, b)
				_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[]}`))
				return
			}
			searches++
			switch searches {
			case 1:
				// Two posts in different ISO weeks.
				_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":2,"relation":"eq"},"hits":[` +
					hit("1", "2026-07-22T10:00:00Z") + `,` + hit("2", "2026-07-28T10:00:00Z") + `]}}`))
			default:
				_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
			}
		}))
		defer srv.Close()

		cfg := QualityBackfillConfig{
			SourceIndex:  "posts_recent",
			Threshold:    20,
			IndexPeriod:  IndexPeriodWeek,
			RetentionAge: 14 * 24 * time.Hour,
			Now:          time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC),
			PageSize:     500,
		}

		stats, err := BackfillQualityPosts(t.Context(), client, NewLogger(false), cfg, false)
		if err != nil {
			t.Fatalf("BackfillQualityPosts: %v", err)
		}
		if stats.Scanned != 2 || stats.Indexed != 2 {
			t.Errorf("stats = %+v, want Scanned=2 Indexed=2", stats)
		}
		if searches != 2 {
			t.Errorf("expected to page until an empty page (2 searches), got %d", searches)
		}

		all := strings.Join(bulkBodies, "\n")
		for _, want := range []string{"posts-quality-2026-w30", "posts-quality-2026-w31"} {
			if !strings.Contains(all, want) {
				t.Errorf("expected a write to %s; bulk bodies: %s", want, all)
			}
		}
	})

	t.Run("dry run scans but never writes", func(t *testing.T) {
		var searches int
		sawBulk := false
		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(200)
			if strings.Contains(r.URL.Path, "_bulk") {
				sawBulk = true
				_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[]}`))
				return
			}
			searches++
			if searches == 1 {
				_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":1,"relation":"eq"},"hits":[` +
					hit("1", "2026-07-28T10:00:00Z") + `]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
		}))
		defer srv.Close()

		cfg := QualityBackfillConfig{SourceIndex: "posts_recent", Threshold: 20,
			IndexPeriod: IndexPeriodWeek, RetentionAge: 14 * 24 * time.Hour,
			Now: time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC), PageSize: 500}

		stats, err := BackfillQualityPosts(t.Context(), client, NewLogger(false), cfg, true)
		if err != nil {
			t.Fatalf("BackfillQualityPosts: %v", err)
		}
		if sawBulk {
			t.Error("dry run must not issue bulk writes")
		}
		if stats.Scanned != 1 {
			t.Errorf("dry run should still report what it would do: %+v", stats)
		}
	})

	t.Run("skips docs with no search vector", func(t *testing.T) {
		var searches int
		client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(200)
			if strings.Contains(r.URL.Path, "_bulk") {
				_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[]}`))
				return
			}
			searches++
			if searches == 1 {
				_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":1,"relation":"eq"},"hits":[` +
					`{"_id":"at://a","_source":{"at_uri":"at://a","author_did":"did:plc:a",` +
					`"created_at":"2026-07-28T10:00:00Z","like_count":25},` +
					`"sort":["2026-07-28T10:00:00Z","2026-07-28T10:00:00Z"]}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
		}))
		defer srv.Close()

		cfg := QualityBackfillConfig{SourceIndex: "posts_recent", Threshold: 20,
			IndexPeriod: IndexPeriodWeek, RetentionAge: 14 * 24 * time.Hour,
			Now: time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC), PageSize: 500}

		stats, err := BackfillQualityPosts(t.Context(), client, NewLogger(false), cfg, false)
		if err != nil {
			t.Fatalf("BackfillQualityPosts: %v", err)
		}
		if stats.Indexed != 0 || stats.Skipped != 1 {
			t.Errorf("stats = %+v, want Indexed=0 Skipped=1", stats)
		}
	})
}
