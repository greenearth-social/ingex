package common

import (
	"net/http"
	"testing"
)

func TestFieldMappingTypes(t *testing.T) {
	// Shape of a real GET /<index>/_mapping/field/<fields> response: keyed by
	// index, then by full field name, with the leaf mapping under the last
	// path segment. perspective_scores.toxicity exercises the nested case.
	const body = `{
	  "posts-2026-w35": {
	    "mappings": {
	      "combined_perspective_score": {
	        "full_name": "combined_perspective_score",
	        "mapping": {"combined_perspective_score": {"type": "float"}}
	      },
	      "perspective_scored_at": {
	        "full_name": "perspective_scored_at",
	        "mapping": {"perspective_scored_at": {"type": "date", "format": "iso8601"}}
	      },
	      "perspective_scores.toxicity": {
	        "full_name": "perspective_scores.toxicity",
	        "mapping": {"toxicity": {"type": "float", "index": false}}
	      }
	    }
	  }
	}`

	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	types, err := FieldMappingTypes(t.Context(), client, "posts-2026-w35",
		[]string{"combined_perspective_score", "perspective_scored_at", "perspective_scores.toxicity"})
	if err != nil {
		t.Fatalf("FieldMappingTypes: %v", err)
	}

	want := map[string]string{
		"combined_perspective_score":  "float",
		"perspective_scored_at":       "date",
		"perspective_scores.toxicity": "float",
	}
	for field, wantType := range want {
		if types[field] != wantType {
			t.Errorf("%s = %q, want %q", field, types[field], wantType)
		}
	}
	if len(types) != len(want) {
		t.Errorf("got %d fields %v, want %d", len(types), types, len(want))
	}
}

// Elasticsearch omits unmapped fields rather than erroring, and "absent" is
// the signal that an index predates the template. Absence must therefore be an
// empty result, not a failure.
func TestFieldMappingTypesOmitsUnmappedFields(t *testing.T) {
	client, srv := newMockESClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"posts-2026-w34":{"mappings":{}}}`))
	}))
	defer srv.Close()

	types, err := FieldMappingTypes(t.Context(), client, "posts-2026-w34",
		[]string{"combined_perspective_score"})
	if err != nil {
		t.Fatalf("FieldMappingTypes: %v", err)
	}
	if len(types) != 0 {
		t.Errorf("got %v, want no mapped fields", types)
	}
}

func TestFieldMappingTypesNoFieldsRequested(t *testing.T) {
	types, err := FieldMappingTypes(t.Context(), nil, "posts-2026-w35", nil)
	if err != nil {
		t.Fatalf("FieldMappingTypes: %v", err)
	}
	if len(types) != 0 {
		t.Errorf("got %v, want empty", types)
	}
}
