package common

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestFetchLikes_UsesStableAtURITieBreaker(t *testing.T) {
	const timestamp = "2026-09-01T12:00:00Z"

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
				likeSearchHit("at://did:plc:a/app.bsky.feed.like/1", timestamp) + `,` +
				likeSearchHit("at://did:plc:a/app.bsky.feed.like/2", timestamp) + `]}}`))
		case len(after) == 3 && after[2] == "at://did:plc:a/app.bsky.feed.like/2":
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":3,"relation":"eq"},"hits":[` +
				likeSearchHit("at://did:plc:a/app.bsky.feed.like/3", timestamp) + `]}}`))
		default:
			_, _ = w.Write([]byte(`{"took":1,"hits":{"total":{"value":3,"relation":"eq"},"hits":[]}}`))
		}
	}))
	defer srv.Close()

	var got []string
	var afterCreatedAt, afterIndexedAt, afterAtURI string
	for {
		response, err := FetchLikes(t.Context(), client, NewLogger(false), "likes", "", "",
			afterCreatedAt, afterIndexedAt, afterAtURI, 2)
		if err != nil {
			t.Fatalf("FetchLikes returned error: %v", err)
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

	want := []string{
		"at://did:plc:a/app.bsky.feed.like/1",
		"at://did:plc:a/app.bsky.feed.like/2",
		"at://did:plc:a/app.bsky.feed.like/3",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d likes %v, want %d likes %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("like %d = %q, want %q", i, got[i], want[i])
		}
	}

	if len(requests) != 3 {
		t.Fatalf("got %d search requests, want 3", len(requests))
	}
	assertLikeSort(t, requests[0])
	assertLikeSearchAfter(t, requests[1], timestamp, "at://did:plc:a/app.bsky.feed.like/2")
	assertLikeSearchAfter(t, requests[2], timestamp, "at://did:plc:a/app.bsky.feed.like/3")
}

func likeSearchHit(atURI, timestamp string) string {
	encoded, _ := json.Marshal(map[string]interface{}{
		"_id": atURI,
		"_source": map[string]string{
			"at_uri":      atURI,
			"subject_uri": "at://did:plc:post/app.bsky.feed.post/1",
			"author_did":  "did:plc:a",
			"created_at":  timestamp,
			"indexed_at":  timestamp,
		},
		"sort": []string{timestamp, timestamp, atURI},
	})
	return string(encoded)
}

func assertLikeSort(t *testing.T, body map[string]interface{}) {
	t.Helper()
	sortFields, ok := body["sort"].([]interface{})
	if !ok || len(sortFields) != 3 {
		t.Fatalf("sort = %v, want three fields", body["sort"])
	}

	want := []string{"created_at", "indexed_at", "at_uri"}
	for i, field := range want {
		sortClause, ok := sortFields[i].(map[string]interface{})
		if !ok || sortClause[field] != "asc" {
			t.Errorf("sort[%d] = %v, want %s ascending", i, sortFields[i], field)
		}
	}
}

func assertLikeSearchAfter(t *testing.T, body map[string]interface{}, timestamp, atURI string) {
	t.Helper()
	after, ok := body["search_after"].([]interface{})
	if !ok || len(after) != 3 {
		t.Fatalf("search_after = %v, want three cursor values", body["search_after"])
	}
	if after[0] != timestamp || after[1] != timestamp || after[2] != atURI {
		t.Errorf("search_after = %v, want [%q %q %q]", after, timestamp, timestamp, atURI)
	}
}
