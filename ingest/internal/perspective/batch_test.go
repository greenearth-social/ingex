package perspective

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// Perspective shares one deadline with the Elasticsearch write, sequentially:
// indexDocuments runs enrich.Wait() and only then BulkIndex, on the same
// context. So an unbounded wait here does not merely delay scoring — it eats
// the budget the bulk index needs, and a timed-out BulkIndex drops the batch's
// posts. Scores are advisory; the posts are not.
func TestScoringBudgetLeavesTimeForTheWrite(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx, cancelBudget := withScoringBudget(parent)
	defer cancelBudget()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("scoring budget left the context without a deadline")
	}
	budget := time.Until(deadline)
	if budget > 600*time.Millisecond {
		t.Errorf("budget = %v of a 1s parent; more than half leaves the write short", budget)
	}
	if budget < 400*time.Millisecond {
		t.Errorf("budget = %v of a 1s parent; scoring was cut too far", budget)
	}
}

// The backfill is what repairs whatever ingest sheds, so it must not shed in
// turn. It runs on a context with no deadline, and gets left alone.
func TestScoringBudgetLeavesAnUnboundedContextAlone(t *testing.T) {
	ctx, cancel := withScoringBudget(context.Background())
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Error("a context with no deadline was given one; the backfill must stay unbounded")
	}
}

// A post still queued when the budget runs out and a post already in flight are
// the same event, and both must leave the post findable by the backfill. Only
// the classification differs in the code, so it is worth pinning: counting an
// expired request as failed would make failed.count mean two things.
func TestBudgetExpiryCountsAsSkippedNotFailed(t *testing.T) {
	// Never answers, so the budget is the only thing that can end the request.
	// released is what lets the handler return at teardown: a client-side
	// cancellation does not reliably unblock the server side, and
	// httptest.Server.Close waits for its handlers.
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-released:
		}
	}))
	defer server.Close()
	defer close(released)

	scorer := NewBatchScorer(testClient(server.URL, 0), 100, 8, QuotaWait, common.NewLogger(false))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	results := scorer.Score(ctx, []ScoreInput{{AtURI: "at://did:plc:a/app.bsky.feed.post/1", Content: "text"}})

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Outcome != OutcomeSkipped {
		t.Errorf("outcome = %v, want OutcomeSkipped", results[0].Outcome)
	}
}
