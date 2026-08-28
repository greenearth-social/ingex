package perspective

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/greenearth/ingest/internal/common"
)

// scoringServer answers every analyze request by delegating to respond, which
// receives the comment text so a test can vary the outcome per post.
func scoringServer(t *testing.T, respond func(text string, w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Comment struct {
				Text string `json:"text"`
			} `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		respond(body.Comment.Text, w)
	}))
	t.Cleanup(server.Close)
	return server
}

func testScorer(host string, qps int, policy QuotaPolicy) *BatchScorer {
	logger := common.NewLogger(false)
	return NewBatchScorer(testClient(host, 0), qps, 8, policy, logger)
}

func postDoc(atURI, content string) common.PostDoc {
	return common.PostDoc{AtURI: atURI, Content: content}
}

func TestAttachPerspectiveScoresSetsAllThreeFields(t *testing.T) {
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.0))
	})

	docs := []common.PostDoc{postDoc("at://did:plc:a/app.bsky.feed.post/1", "hello")}
	scored, unscorable, skipped, failed := AttachPerspectiveScores(context.Background(), testScorer(server.URL, 100, QuotaWait), docs)

	if scored != 1 || unscorable != 0 || skipped != 0 || failed != 0 {
		t.Fatalf("counts = %d/%d/%d/%d, want 1/0/0/0", scored, unscorable, skipped, failed)
	}
	if len(docs[0].PerspectiveScores) != len(RequestedAttributes) {
		t.Errorf("got %d attribute scores, want %d", len(docs[0].PerspectiveScores), len(RequestedAttributes))
	}
	if docs[0].CombinedPerspectiveScore == nil {
		t.Fatal("combined score is nil")
	}
	// All attributes at 0.0 is the neutral midpoint. It is also the case a
	// non-pointer float64 with omitempty would have dropped from the document.
	if got := *docs[0].CombinedPerspectiveScore; got != 0.5 {
		t.Errorf("combined score = %v, want 0.5", got)
	}
	if docs[0].PerspectiveScoredAt == "" {
		t.Error("perspective_scored_at was not stamped")
	}
}

// A zero combined score is meaningful (maximally toxic) and must survive
// serialization — the reason the field is a pointer.
func TestZeroCombinedScoreSurvivesMarshalling(t *testing.T) {
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		full := fullScoreResponse(0.0)
		scores, ok := full["attributeScores"].(map[string]interface{})
		if !ok {
			t.Fatal("fullScoreResponse did not return an attributeScores map")
		}
		for _, name := range RequestedAttributes {
			if prcWeights[name] < 0 {
				scores[name] = map[string]interface{}{
					"summaryScore": map[string]interface{}{"value": 1.0},
				}
			}
		}
		_ = json.NewEncoder(w).Encode(full)
	})

	docs := []common.PostDoc{postDoc("at://did:plc:a/app.bsky.feed.post/1", "vile")}
	AttachPerspectiveScores(context.Background(), testScorer(server.URL, 100, QuotaWait), docs)

	if docs[0].CombinedPerspectiveScore == nil || *docs[0].CombinedPerspectiveScore != 0.0 {
		t.Fatalf("combined score = %v, want 0.0", docs[0].CombinedPerspectiveScore)
	}
	encoded, err := json.Marshal(docs[0])
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if !strings.Contains(string(encoded), `"combined_perspective_score":0`) {
		t.Errorf("a 0.0 score was omitted from the document: %s", encoded)
	}
}

// Unscorable text is stamped but unscored. The stamp is what stops the api
// re-querying every non-English post on every request forever.
func TestAttachPerspectiveScoresStampsUnsupportedLanguage(t *testing.T) {
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"details":[{"errorType":"LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE"}]}}`))
	})

	docs := []common.PostDoc{postDoc("at://did:plc:a/app.bsky.feed.post/1", "日本語")}
	scored, unscorable, _, _ := AttachPerspectiveScores(context.Background(), testScorer(server.URL, 100, QuotaWait), docs)

	if scored != 0 || unscorable != 1 {
		t.Fatalf("counts = %d scored, %d unscorable; want 0, 1", scored, unscorable)
	}
	if docs[0].PerspectiveScoredAt == "" {
		t.Error("an unscorable post must still be stamped, or it is retried forever")
	}
	if docs[0].CombinedPerspectiveScore != nil {
		t.Error("an unscorable post must have no score")
	}
	if docs[0].PerspectiveScores != nil {
		t.Error("an unscorable post must have no attribute scores")
	}
}

// A failure leaves the document unstamped, so it still indexes and the
// backfill can find it later.
func TestAttachPerspectiveScoresFailsOpen(t *testing.T) {
	server := scoringServer(t, func(text string, w http.ResponseWriter) {
		if text == "bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	})

	docs := []common.PostDoc{
		postDoc("at://did:plc:a/app.bsky.feed.post/1", "good"),
		postDoc("at://did:plc:a/app.bsky.feed.post/2", "bad"),
		postDoc("at://did:plc:a/app.bsky.feed.post/3", "good"),
	}
	scored, _, _, failed := AttachPerspectiveScores(context.Background(), testScorer(server.URL, 100, QuotaWait), docs)

	if scored != 2 || failed != 1 {
		t.Fatalf("counts = %d scored, %d failed; want 2, 1", scored, failed)
	}
	// One post's failure must not cost the others their scores.
	for _, i := range []int{0, 2} {
		if docs[i].CombinedPerspectiveScore == nil {
			t.Errorf("doc %d lost its score to a sibling's failure", i)
		}
	}
	if docs[1].PerspectiveScoredAt != "" || docs[1].CombinedPerspectiveScore != nil {
		t.Error("a failed post must be left entirely unmarked so a backfill finds it")
	}
}

// A post with no text is never sent — there is nothing to score — but it *is*
// stamped. Leaving it unstamped would keep it in the backfill's scan forever,
// and since the scan runs oldest-first, image-only posts would crowd out the
// posts that actually need scoring.
func TestAttachPerspectiveScoresStampsContentlessPosts(t *testing.T) {
	var requests atomic.Int32
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	})

	docs := []common.PostDoc{
		postDoc("at://did:plc:a/app.bsky.feed.post/1", ""),
		postDoc("at://did:plc:a/app.bsky.feed.post/2", "   \n  "),
		postDoc("at://did:plc:a/app.bsky.feed.post/3", "text"),
	}
	scored, unscorable, skipped, _ := AttachPerspectiveScores(context.Background(), testScorer(server.URL, 100, QuotaWait), docs)

	if scored != 1 || unscorable != 2 || skipped != 0 {
		t.Fatalf("counts = %d scored, %d unscorable, %d skipped; want 1, 2, 0", scored, unscorable, skipped)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("made %d requests, want 1 — contentless posts must not be sent", got)
	}
	for _, i := range []int{0, 1} {
		if docs[i].PerspectiveScoredAt == "" {
			t.Errorf("doc %d was not stamped; it will be rescanned forever", i)
		}
		if docs[i].CombinedPerspectiveScore != nil {
			t.Errorf("doc %d has no text and must have no score", i)
		}
	}
}

// A nil scorer is how the kill switch is expressed: no scoring, no other
// change to the documents.
func TestAttachPerspectiveScoresNilScorerIsNoOp(t *testing.T) {
	docs := []common.PostDoc{postDoc("at://did:plc:a/app.bsky.feed.post/1", "text")}
	scored, unscorable, skipped, failed := AttachPerspectiveScores(context.Background(), nil, docs)

	if scored|unscorable|skipped|failed != 0 {
		t.Errorf("counts = %d/%d/%d/%d, want all zero", scored, unscorable, skipped, failed)
	}
	if docs[0].PerspectiveScoredAt != "" || docs[0].CombinedPerspectiveScore != nil {
		t.Error("a nil scorer must leave documents untouched")
	}
}

// Under skip mode the batch is not delayed; posts past the budget are simply
// left for the backfill.
func TestAttachPerspectiveScoresSkipModeLeavesPostsUnscored(t *testing.T) {
	server := scoringServer(t, func(_ string, w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	})

	docs := make([]common.PostDoc, 5)
	for i := range docs {
		docs[i] = postDoc("at://did:plc:a/app.bsky.feed.post/"+string(rune('1'+i)), "text")
	}
	// Budget of 1/s with burst 1: one post gets through, the rest are dropped.
	scored, _, skipped, _ := AttachPerspectiveScores(context.Background(), testScorer(server.URL, 1, QuotaSkip), docs)

	if scored != 1 {
		t.Errorf("scored %d posts, want 1 (the burst allowance)", scored)
	}
	if skipped != 4 {
		t.Errorf("skipped %d posts, want 4", skipped)
	}
	unmarked := 0
	for _, doc := range docs {
		if doc.PerspectiveScoredAt == "" {
			unmarked++
		}
	}
	if unmarked != 4 {
		t.Errorf("%d posts left unmarked, want 4 — skipped posts must be findable by the backfill", unmarked)
	}
}

func TestScoreEmptyBatch(t *testing.T) {
	scorer := testScorer("http://127.0.0.1:1", 100, QuotaWait)
	if got := scorer.Score(context.Background(), nil); len(got) != 0 {
		t.Errorf("Score(nil) returned %d results", len(got))
	}
}

func TestScoreResultsAreInInputOrder(t *testing.T) {
	server := scoringServer(t, func(text string, w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	})

	inputs := make([]ScoreInput, 50)
	for i := range inputs {
		inputs[i] = ScoreInput{AtURI: "at://post/" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Content: "text"}
	}
	results := testScorer(server.URL, 1000, QuotaWait).Score(context.Background(), inputs)

	if len(results) != len(inputs) {
		t.Fatalf("got %d results for %d inputs", len(results), len(inputs))
	}
	// Results are matched back to documents by position, so a reordering here
	// would attach every score to the wrong post.
	for i, result := range results {
		if result.AtURI != inputs[i].AtURI {
			t.Fatalf("result %d is for %q, want %q", i, result.AtURI, inputs[i].AtURI)
		}
	}
}
