package perspective

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/greenearth/ingest/internal/common"
)

// fakeES answers search requests from a queue of canned pages and records every
// bulk body it receives.
//
// It honours the request's "size" and echoes one bulk item per queued update,
// because the backfill's paging and its Updated count both read those back —
// a fake that ignored them would let a paging bug pass.
type fakeES struct {
	pages [][]string
	// emptyContent serves hits with no text, standing in for image-only posts.
	emptyContent bool
	// omitIndex serves hits with no _index, which should never happen from a
	// real search and must not result in a write.
	omitIndex  bool
	pageIndex  int
	bulkBodies []string
}

func (f *fakeES) client(t *testing.T) *elasticsearch.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(200)

		if strings.Contains(r.URL.Path, "_bulk") {
			f.bulkBodies = append(f.bulkBodies, string(body))
			// A bulk update is two lines per document.
			docs := len(strings.Split(strings.TrimSpace(string(body)), "\n")) / 2
			items := make([]string, docs)
			for i := range items {
				items[i] = `{"update":{"_id":"x","status":200}}`
			}
			_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[` + strings.Join(items, ",") + `]}`))
			return
		}

		var query struct {
			Size int `json:"size"`
		}
		_ = json.Unmarshal(body, &query)

		var uris []string
		if f.pageIndex < len(f.pages) {
			uris = f.pages[f.pageIndex]
			f.pageIndex++
		}
		if query.Size > 0 && len(uris) > query.Size {
			uris = uris[:query.Size]
		}
		content := "text"
		if f.emptyContent {
			content = ""
		}
		page := searchPage(content, uris...)
		if f.omitIndex {
			page = stripHitIndices(page)
		}
		_, _ = w.Write([]byte(page))
	}))
	t.Cleanup(srv.Close)

	client, err := elasticsearch.New(elasticsearch.WithAddresses(srv.URL))
	if err != nil {
		t.Fatalf("elasticsearch.New: %v", err)
	}
	return client
}

// searchPage builds a scan page. Each hit reports a different concrete
// _index, because that is the shape prod has: the scan is given the
// posts_recent alias, which spans one index per retention period, so a single
// page routinely straddles several. A page whose hits all share one index —
// which is all the devenv can produce, having one posts index — cannot show
// whether the writes are routed per document.
func searchPage(content string, atURIs ...string) string {
	hits := make([]string, len(atURIs))
	for i, uri := range atURIs {
		hits[i] = `{"_index":"` + hitIndex(i) + `","_source":{"at_uri":"` + uri + `","content":"` + content + `",` +
			`"created_at":"2026-08-0` + string(rune('1'+i)) + `T00:00:00Z",` +
			`"indexed_at":"2026-08-0` + string(rune('1'+i)) + `T00:00:01Z"}}`
	}
	return `{"hits":{"hits":[` + strings.Join(hits, ",") + `]}}`
}

// hitIndex names the weekly index the i-th hit of a page lives in.
func hitIndex(i int) string {
	return `posts-2026-w3` + string(rune('1'+i))
}

// stripHitIndices removes the _index from every hit, for the case a search
// somehow reports none.
func stripHitIndices(page string) string {
	for i := 0; i < 10; i++ {
		page = strings.ReplaceAll(page, `{"_index":"`+hitIndex(i)+`",`, `{`)
	}
	return page
}

func alwaysScores(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.5))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The backfill scans through the posts_recent alias but must write to the
// concrete index each document lives in. A bulk update addressed to an alias
// is routed to that alias's *write* index, so passing the alias meant every
// document outside the current period came back document_missing — and since
// the write path counts a 404 as routine ("aged out between scan and write"),
// nothing surfaced. In prod, with a weekly index over 60 days of retention,
// that is roughly eight of every nine posts scored and then dropped.
func TestBackfillWritesToEachDocumentsOwnIndex(t *testing.T) {
	es := &fakeES{pages: [][]string{
		{"at://did:plc:a/app.bsky.feed.post/1", "at://did:plc:b/app.bsky.feed.post/2"},
	}}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 2}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Updated != 2 {
		t.Fatalf("updated %d of 2 posts; stats = %+v", stats.Updated, stats)
	}
	if len(es.bulkBodies) != 1 {
		t.Fatalf("wrote %d bulk requests, want 1", len(es.bulkBodies))
	}

	// One request, but a distinct _index per action — the bulk API takes
	// _index per action, so spanning periods needs no extra round trips.
	var gotIndices []string
	for _, line := range strings.Split(strings.TrimSpace(es.bulkBodies[0]), "\n") {
		var action map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(line), &action); err != nil {
			continue
		}
		meta, ok := action["update"]
		if !ok {
			continue
		}
		idx, _ := meta["_index"].(string)
		gotIndices = append(gotIndices, idx)
	}

	want := []string{hitIndex(0), hitIndex(1)}
	if len(gotIndices) != len(want) {
		t.Fatalf("got %d update actions %v, want %d", len(gotIndices), gotIndices, len(want))
	}
	for i := range want {
		if gotIndices[i] != want[i] {
			t.Errorf("action %d wrote to %q, want %q", i, gotIndices[i], want[i])
		}
	}
	for _, idx := range gotIndices {
		if idx == "posts_recent" {
			t.Error("wrote to the alias; the update would land on its write index, not the document's")
		}
	}
}

// An update with no index is a caller bug, not bad data: it must be dropped
// rather than sent somewhere arbitrary.
func TestBackfillSkipsHitsWithNoIndex(t *testing.T) {
	es := &fakeES{
		pages:     [][]string{{"at://did:plc:a/app.bsky.feed.post/1"}},
		omitIndex: true,
	}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 1}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Updated != 0 {
		t.Errorf("updated %d posts from hits with no _index, want 0", stats.Updated)
	}
}

func TestBackfillScoresAndWrites(t *testing.T) {
	es := &fakeES{pages: [][]string{
		{"at://did:plc:a/app.bsky.feed.post/1", "at://did:plc:b/app.bsky.feed.post/2"},
	}}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 2}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	if stats.Scanned != 2 || stats.Scored != 2 || stats.Updated != 2 {
		t.Errorf("stats = %+v, want 2 scanned, 2 scored, 2 updated", stats)
	}
	if len(es.bulkBodies) != 1 {
		t.Fatalf("wrote %d bulk requests, want 1", len(es.bulkBodies))
	}
	if !strings.Contains(es.bulkBodies[0], "combined_perspective_score") {
		t.Errorf("bulk body carries no scores: %s", es.bulkBodies[0])
	}
}

// A short page means the scan is done. Without this the loop would keep
// querying past the end of the data.
func TestBackfillStopsOnShortPage(t *testing.T) {
	es := &fakeES{pages: [][]string{
		{"at://did:plc:a/app.bsky.feed.post/1"},
		{"at://did:plc:b/app.bsky.feed.post/2"},
	}}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 10}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Pages != 1 {
		t.Errorf("read %d pages, want 1 — a page shorter than PageSize is the last one", stats.Pages)
	}
}

// MaxPosts bounds a single run. A backfill over a large window otherwise runs
// for hours and, sharing the quota with serving, is not something you would
// want to start without a way to bound it.
func TestBackfillRespectsMaxPosts(t *testing.T) {
	es := &fakeES{pages: [][]string{
		{"at://did:plc:a/app.bsky.feed.post/1", "at://did:plc:b/app.bsky.feed.post/2"},
		{"at://did:plc:c/app.bsky.feed.post/3", "at://did:plc:d/app.bsky.feed.post/4"},
		{"at://did:plc:e/app.bsky.feed.post/5", "at://did:plc:f/app.bsky.feed.post/6"},
	}}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 2, MaxPosts: 3}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Scanned > 3 {
		t.Errorf("scanned %d posts, want at most 3", stats.Scanned)
	}
}

// Unscorable posts are still written — the stamp is the whole point, since it
// is what stops them being rescanned on every subsequent run.
func TestBackfillStampsUnsupportedLanguage(t *testing.T) {
	es := &fakeES{pages: [][]string{{"at://did:plc:a/app.bsky.feed.post/1"}}}
	scoring := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"details":[{"errorType":"LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE"}]}}`))
	}))
	defer scoring.Close()

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 1}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Unscorable != 1 || stats.Updated != 1 {
		t.Errorf("stats = %+v, want 1 unscorable and 1 written", stats)
	}
	if len(es.bulkBodies) != 1 || strings.Contains(es.bulkBodies[0], "combined_perspective_score") {
		t.Errorf("unscorable post should be written as a bare stamp: %v", es.bulkBodies)
	}
}

// A failed post is not written at all, so the next run finds it again.
func TestBackfillLeavesFailedPostsUnwritten(t *testing.T) {
	es := &fakeES{pages: [][]string{{"at://did:plc:a/app.bsky.feed.post/1"}}}
	scoring := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer scoring.Close()

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 1}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Failed != 1 || stats.Updated != 0 {
		t.Errorf("stats = %+v, want 1 failed and nothing written", stats)
	}
	if len(es.bulkBodies) != 0 {
		t.Errorf("a failed post must not be stamped, or it is never retried: %v", es.bulkBodies)
	}
}

func TestBackfillDryRunWritesNothing(t *testing.T) {
	es := &fakeES{pages: [][]string{{"at://did:plc:a/app.bsky.feed.post/1"}}}
	scoring := alwaysScores(t)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false),
		testScorer(scoring.URL, 1000, QuotaWait),
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 1}, true)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Scored != 1 {
		t.Errorf("a dry run should still score, so the run is representative: %+v", stats)
	}
	if len(es.bulkBodies) != 0 {
		t.Errorf("dry run wrote to Elasticsearch: %v", es.bulkBodies)
	}
}

// Contentless posts are written as bare stamps, which is what lets a repeated
// backfill converge. Without this they stay in the scan forever and, because
// the scan is oldest-first, a bounded run never reaches the posts that need
// scoring at all.
func TestBackfillStampsContentlessPostsSoTheScanConverges(t *testing.T) {
	es := &fakeES{pages: [][]string{{"at://did:plc:a/app.bsky.feed.post/1"}}}
	es.emptyContent = true
	// No scoring server: a contentless post must never reach one.
	scorer := testScorer("http://127.0.0.1:1", 1000, QuotaWait)

	stats, err := Backfill(t.Context(), es.client(t), common.NewLogger(false), scorer,
		BackfillConfig{SourceIndex: "posts_recent", PageSize: 1}, false)
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if stats.Unscorable != 1 || stats.Updated != 1 {
		t.Errorf("stats = %+v, want 1 unscorable and 1 written", stats)
	}
	if len(es.bulkBodies) != 1 {
		t.Fatalf("wrote %d bulk requests, want 1", len(es.bulkBodies))
	}
	if !strings.Contains(es.bulkBodies[0], "perspective_scored_at") {
		t.Errorf("contentless post was not stamped: %s", es.bulkBodies[0])
	}
	if strings.Contains(es.bulkBodies[0], "combined_perspective_score") {
		t.Errorf("contentless post was given a score: %s", es.bulkBodies[0])
	}
}
