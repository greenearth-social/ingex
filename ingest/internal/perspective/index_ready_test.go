package perspective

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/greenearth/ingest/internal/common"
)

// The mapping the posts index template produces.
func templateTypes() map[string]string {
	return map[string]string{
		"combined_perspective_score":  "float",
		"perspective_scored_at":       "date",
		"perspective_scores.toxicity": "float",
	}
}

func TestIndexMappingReadyOnTemplateMapping(t *testing.T) {
	ready, reason := IndexMappingReady(templateTypes())
	if !ready {
		t.Errorf("template mapping reported not ready: %s", reason)
	}
}

// An index created before the template gained these fields simply does not map
// them: Elasticsearch omits unmapped fields from the field-mapping API.
func TestIndexMappingNotReadyWhenFieldsAbsent(t *testing.T) {
	for _, missing := range []string{
		"combined_perspective_score",
		"perspective_scored_at",
		"perspective_scores.toxicity",
	} {
		types := templateTypes()
		delete(types, missing)
		ready, reason := IndexMappingReady(types)
		if ready {
			t.Errorf("reported ready with %s unmapped", missing)
		}
		if !strings.Contains(reason, missing) {
			t.Errorf("reason %q does not name the missing field %s", reason, missing)
		}
	}
}

func TestIndexMappingNotReadyOnEmptyMapping(t *testing.T) {
	ready, reason := IndexMappingReady(map[string]string{})
	if ready {
		t.Error("an index mapping none of the fields reported ready")
	}
	for field := range RequiredIndexFields {
		if !strings.Contains(reason, field) {
			t.Errorf("reason %q omits %s", reason, field)
		}
	}
}

// The case the gate exists for: Go renders float64(0) as the JSON integer 0,
// so a maximally toxic post arriving first into an unmapped index makes
// Elasticsearch infer long, and every fractional score after it is truncated
// to 0 in the index. Such an index must never be written to again.
func TestIndexMappingNotReadyWhenScoreDynamicallyMappedAsLong(t *testing.T) {
	types := templateTypes()
	types["combined_perspective_score"] = "long"

	ready, reason := IndexMappingReady(types)
	if ready {
		t.Fatal("reported ready for an index that mapped the score as long")
	}
	if !strings.Contains(reason, "long") || !strings.Contains(reason, "float") {
		t.Errorf("reason %q should name both the actual and wanted type", reason)
	}
}

func TestIndexMappingNotReadyWhenTimestampMappedAsText(t *testing.T) {
	types := templateTypes()
	types["perspective_scored_at"] = "text"
	if ready, _ := IndexMappingReady(types); ready {
		t.Error("reported ready with perspective_scored_at mapped as text")
	}
}

// A scorer must not write anything until the gate is opened, so a deploy that
// lands before the template cannot poison the index it is writing to.
func TestAttachPerspectiveScoresIsNoOpUntilGateOpens(t *testing.T) {
	var calls int
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		calls++
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	})

	scorer := NewBatchScorer(testClient(server.URL, 0), 100, 8, QuotaWait, common.NewLogger(false))
	if scorer.IndexReady() {
		t.Fatal("a new scorer starts with the gate open; it must start closed")
	}

	docs := []common.PostDoc{postDoc("at://did:plc:a/app.bsky.feed.post/1", "hello")}
	scored, unscorable, skipped, failed := AttachPerspectiveScores(context.Background(), scorer, docs)

	if scored+unscorable+skipped+failed != 0 {
		t.Errorf("counts = %d/%d/%d/%d, want all zero while gated", scored, unscorable, skipped, failed)
	}
	if calls != 0 {
		t.Errorf("made %d Perspective calls while gated, want 0", calls)
	}
	// No fields written at all: indistinguishable from never having been
	// scored, which is what lets the api score it live and the backfill
	// collect it later.
	if docs[0].PerspectiveScoredAt != "" || docs[0].CombinedPerspectiveScore != nil || docs[0].PerspectiveScores != nil {
		t.Errorf("gated scoring wrote fields: %+v", docs[0])
	}

	// Opening the gate starts scoring, with no other change.
	scorer.SetIndexReady(true)
	scored, _, _, _ = AttachPerspectiveScores(context.Background(), scorer, docs)
	if scored != 1 {
		t.Errorf("scored %d after opening the gate, want 1", scored)
	}
	if docs[0].CombinedPerspectiveScore == nil {
		t.Error("no score written after opening the gate")
	}
}
