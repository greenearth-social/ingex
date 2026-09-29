package common

import (
	"encoding/json"
	"net/http"
	"testing"
)

// search_after resumes strictly *after* the cursor, so a sort that is not a
// total order loses documents: when a page boundary falls inside a group
// sharing the same sort key, every remaining member of that group is skipped.
// created_at has second granularity and posts arrive in bursts, so ties across
// a boundary are routine rather than exotic -- the same defect measured 3-4%
// loss in FetchLikes and ~2% in the Perspective scan. at_uri is unique, which
// makes (created_at, indexed_at, at_uri) total and the paging exact.
//
// Every post below shares one timestamp, so only the at_uri tie-breaker can
// page through them.
func TestFetchPosts_UsesStableAtURITieBreaker(t *testing.T) {
	const timestamp = "2026-09-01T12:00:00Z"
	uri := func(n string) string { return "at://did:plc:a/app.bsky.feed.post/" + n }

	var requests []map[string]interface{}
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		requests = append(requests, body)

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)

		after, _ := body["search_after"].([]interface{})
		switch {
		case len(after) == 0:
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":3,"relation":"eq"},"hits":[` +
				postSearchHit(uri("1"), timestamp) + `,` +
				postSearchHit(uri("2"), timestamp) + `]}}`))
		case len(after) == 3 && after[2] == uri("2"):
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":3,"relation":"eq"},"hits":[` +
				postSearchHit(uri("3"), timestamp) + `]}}`))
		default:
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":3,"relation":"eq"},"hits":[]}}`))
		}
	}))
	defer srv.Close()

	var got []string
	var afterCreatedAt, afterIndexedAt, afterAtURI string
	for {
		response, err := FetchPosts(t.Context(), client, NewLogger(false), "posts-2026-w35", "", "",
			afterCreatedAt, afterIndexedAt, afterAtURI, 2)
		if err != nil {
			t.Fatalf("FetchPosts returned error: %v", err)
		}
		if len(response.Hits.Hits) == 0 {
			break
		}
		for _, hit := range response.Hits.Hits {
			got = append(got, hit.Source.AtURI)
		}
		last := response.Hits.Hits[len(response.Hits.Hits)-1]
		afterCreatedAt = last.Source.CreatedAt
		afterIndexedAt = last.Source.IndexedAt
		afterAtURI = last.Source.AtURI
	}

	want := []string{uri("1"), uri("2"), uri("3")}
	if len(got) != len(want) {
		t.Fatalf("got %d posts %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("post %d = %q, want %q", i, got[i], want[i])
		}
	}

	if len(requests) != 3 {
		t.Fatalf("got %d search requests, want 3", len(requests))
	}
	assertPostSort(t, requests[0])
	assertPostSearchAfter(t, requests[1], timestamp, uri("2"))
	assertPostSearchAfter(t, requests[2], timestamp, uri("3"))
}

// An incomplete cursor must not fall back to an unpaged query: that silently
// restarts the scan from the beginning on every page.
func TestFetchPosts_IncompleteCursorSendsNoSearchAfter(t *testing.T) {
	var body map[string]interface{}
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer srv.Close()

	_, err := FetchPosts(t.Context(), client, NewLogger(false), "posts-2026-w35", "", "",
		"2026-09-01T12:00:00Z", "2026-09-01T12:00:00Z", "", 10)
	if err != nil {
		t.Fatalf("FetchPosts returned error: %v", err)
	}
	if _, present := body["search_after"]; present {
		t.Errorf("an incomplete cursor sent search_after = %v", body["search_after"])
	}
}

func postSearchHit(atURI, timestamp string) string {
	encoded, _ := json.Marshal(map[string]interface{}{
		"_id": atURI,
		"_source": map[string]interface{}{
			"at_uri":     atURI,
			"author_did": "did:plc:a",
			"content":    "hello",
			"created_at": timestamp,
			"indexed_at": timestamp,
			"like_count": 3,
		},
		"sort": []string{timestamp, timestamp, atURI},
	})
	return string(encoded)
}

func assertPostSort(t *testing.T, body map[string]interface{}) {
	t.Helper()
	sortFields, ok := body["sort"].([]interface{})
	if !ok || len(sortFields) != 3 {
		t.Fatalf("sort = %v, want three fields", body["sort"])
	}
	for i, field := range []string{"created_at", "indexed_at", "at_uri"} {
		sortClause, ok := sortFields[i].(map[string]interface{})
		if !ok || sortClause[field] != "asc" {
			t.Errorf("sort[%d] = %v, want %s ascending", i, sortFields[i], field)
		}
	}
}

func assertPostSearchAfter(t *testing.T, body map[string]interface{}, timestamp, atURI string) {
	t.Helper()
	after, ok := body["search_after"].([]interface{})
	if !ok || len(after) != 3 {
		t.Fatalf("search_after = %v, want three cursor values", body["search_after"])
	}
	if after[0] != timestamp || after[1] != timestamp || after[2] != atURI {
		t.Errorf("search_after = %v, want [%q %q %q]", after, timestamp, timestamp, atURI)
	}
}
