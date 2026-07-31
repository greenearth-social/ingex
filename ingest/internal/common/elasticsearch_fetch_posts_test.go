package common

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestEmbeddingsFromHit covers the fallback chain used because posts/replies
// templates exclude "embeddings" from _source (greenearth-social/api#312 step 2):
// once that happens, embeddings are only retrievable via the ES "docvalue_fields"
// API, not _source. Both "fields" and "docvalue_fields" populate the same
// response key ("fields"), so these fixtures apply regardless of which request
// param was used to ask for them.
func TestEmbeddingsFromHit(t *testing.T) {
	t.Run("prefers fields when present", func(t *testing.T) {
		hit := Hit{
			Source: PostData{Embeddings: nil},
			Fields: map[string]json.RawMessage{
				"embeddings.all_MiniLM_L12_v2": json.RawMessage(`[[0.1,0.2,0.3]]`),
			},
		}
		got := embeddingsFromHit(hit)
		want := []float32{0.1, 0.2, 0.3}
		if len(got["all_MiniLM_L12_v2"]) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i, v := range want {
			if got["all_MiniLM_L12_v2"][i] != v {
				t.Errorf("index %d: got %v, want %v", i, got["all_MiniLM_L12_v2"][i], v)
			}
		}
	})

	t.Run("falls back to _source when no fields present", func(t *testing.T) {
		hit := Hit{
			Source: PostData{
				Embeddings: map[string][]float32{"all_MiniLM_L12_v2": {1, 2, 3}},
			},
		}
		got := embeddingsFromHit(hit)
		if len(got["all_MiniLM_L12_v2"]) != 3 {
			t.Fatalf("expected fallback to _source embeddings, got %v", got)
		}
	})

	t.Run("falls back to _source when fields has no embeddings keys", func(t *testing.T) {
		hit := Hit{
			Source: PostData{
				Embeddings: map[string][]float32{"all_MiniLM_L12_v2": {1, 2, 3}},
			},
			Fields: map[string]json.RawMessage{
				"some_other_field": json.RawMessage(`["x"]`),
			},
		}
		got := embeddingsFromHit(hit)
		if len(got["all_MiniLM_L12_v2"]) != 3 {
			t.Fatalf("expected fallback to _source embeddings, got %v", got)
		}
	})

	t.Run("multiple embedding families from fields", func(t *testing.T) {
		hit := Hit{
			Fields: map[string]json.RawMessage{
				"embeddings.all_MiniLM_L12_v2":          json.RawMessage(`[[0.1,0.2]]`),
				"embeddings.google_embeddinggemma_300m": json.RawMessage(`[[0.9,0.8]]`),
				"embeddings.ge_post_embedding":          json.RawMessage(`[[0.4,0.5]]`),
			},
		}
		got := embeddingsFromHit(hit)
		if len(got) != 3 {
			t.Fatalf("expected 3 embedding families, got %v", got)
		}
	})

	t.Run("no embeddings anywhere returns empty map", func(t *testing.T) {
		hit := Hit{Source: PostData{}}
		got := embeddingsFromHit(hit)
		if len(got) != 0 {
			t.Errorf("expected empty map, got %v", got)
		}
	})
}

// TestFetchPosts_RequestsEmbeddingsViaFields verifies FetchPosts asks for
// embeddings through the "docvalue_fields" retrieval API (not "fields" and
// not just _source): "fields" falls back to decompressing _source for
// dense_vector on this ES version, so it silently returns nothing once
// _source excludes the field (see greenearth-social/api#325).
func TestFetchPosts_RequestsEmbeddingsViaFields(t *testing.T) {
	var capturedBody map[string]interface{}

	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":0,"hits":[]}}`))
	}))
	defer srv.Close()

	logger := NewLogger(false)
	_, err := FetchPosts(t.Context(), client, logger, "posts-2026-w30", "", "", "", "", 10)
	if err != nil {
		t.Fatalf("FetchPosts returned error: %v", err)
	}

	fields, ok := capturedBody["docvalue_fields"].([]interface{})
	if !ok || len(fields) == 0 {
		t.Fatalf("expected query to request \"docvalue_fields\", got: %v", capturedBody)
	}
	if fields[0] != "embeddings.*" {
		t.Errorf("expected docvalue_fields=[\"embeddings.*\"], got %v", fields)
	}
	if _, ok := capturedBody["fields"]; ok {
		t.Errorf("expected no \"fields\" param (silently returns nothing for dense_vector once _source excludes it), got: %v", capturedBody)
	}
}
