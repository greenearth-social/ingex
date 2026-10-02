package jetstream_ingest

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// fakeFollowStore records the writes the writer would make to Firestore.
type fakeFollowStore struct {
	mu          sync.Mutex
	appends     [][2]string // {userDocID, subjectDID}
	invalidated []string
	appendErr   error
}

func (f *fakeFollowStore) AppendPendingFollow(ctx context.Context, userDocID, subjectDID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appends = append(f.appends, [2]string{userDocID, subjectDID})
	return nil
}

func (f *fakeFollowStore) InvalidateFollows(ctx context.Context, userDocID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated = append(f.invalidated, userDocID)
	return nil
}

func (f *fakeFollowStore) snapshot() ([][2]string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]string(nil), f.appends...), append([]string(nil), f.invalidated...)
}

type fakeUserLister struct {
	mu     sync.Mutex
	dids   []string
	err    error
	calls  int
	notify chan struct{}
}

func (f *fakeUserLister) ListUserDIDs(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.notify != nil {
		select {
		case f.notify <- struct{}{}:
		default:
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.dids...), nil
}

func makeFollow(did, subject string) common.JetstreamMessage {
	return common.NewJetstreamMessage(`{
		"did": "`+did+`",
		"kind": "commit",
		"commit": {
			"operation": "create",
			"collection": "app.bsky.graph.follow",
			"rkey": "abc",
			"record": {"subject": "`+subject+`"}
		}
	}`, common.NewLogger(false))
}

func makeUnfollow(did string) common.JetstreamMessage {
	return common.NewJetstreamMessage(`{
		"did": "`+did+`",
		"kind": "commit",
		"commit": {
			"operation": "delete",
			"collection": "app.bsky.graph.follow",
			"rkey": "abc"
		}
	}`, common.NewLogger(false))
}

// drain runs the writer until it has processed everything enqueued so far.
func drain(t *testing.T, w *FollowWriter) {
	t.Helper()
	w.Close()
	select {
	case <-w.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("follow writer did not finish")
	}
}

func newTestWriter(store followStore, tracked *TrackedUsers) *FollowWriter {
	w := NewFollowWriter(store, tracked, common.NewLogger(false), 16)
	go w.Run(context.Background())
	return w
}

func TestFollowWriter_AppendsFollowsForTrackedUsers(t *testing.T) {
	store := &fakeFollowStore{}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	w := newTestWriter(store, tracked)

	w.Enqueue(makeFollow("did:plc:ours", "did:plc:newfollow"))
	drain(t, w)

	appends, _ := store.snapshot()
	if len(appends) != 1 || appends[0] != [2]string{"ours", "did:plc:newfollow"} {
		t.Fatalf("appends = %v, want one {ours did:plc:newfollow}", appends)
	}
}

func TestFollowWriter_IgnoresUsersWeDoNotServe(t *testing.T) {
	// The firehose carries every follow on the network. Filtering to our own
	// users before writing is what keeps Firestore write volume negligible.
	store := &fakeFollowStore{}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	w := newTestWriter(store, tracked)

	w.Enqueue(makeFollow("did:plc:stranger", "did:plc:whoever"))
	w.Enqueue(makeUnfollow("did:plc:stranger"))
	drain(t, w)

	appends, invalidated := store.snapshot()
	if len(appends) != 0 || len(invalidated) != 0 {
		t.Fatalf("wrote for an untracked user: appends=%v invalidated=%v", appends, invalidated)
	}
}

func TestFollowWriter_UnfollowInvalidatesTheEntry(t *testing.T) {
	store := &fakeFollowStore{}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	w := newTestWriter(store, tracked)

	w.Enqueue(makeUnfollow("did:plc:ours"))
	drain(t, w)

	_, invalidated := store.snapshot()
	if len(invalidated) != 1 || invalidated[0] != "ours" {
		t.Fatalf("invalidated = %v, want [ours]", invalidated)
	}
}

func TestFollowWriter_NonFollowEventsAreIgnored(t *testing.T) {
	store := &fakeFollowStore{}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	w := newTestWriter(store, tracked)

	like := common.NewJetstreamMessage(`{
		"did": "did:plc:ours",
		"kind": "commit",
		"commit": {
			"operation": "create",
			"collection": "app.bsky.feed.like",
			"rkey": "abc",
			"record": {"subject": {"uri": "at://x/app.bsky.feed.post/y"}, "createdAt": "2025-01-27T12:34:56.789Z"}
		}
	}`, common.NewLogger(false))
	w.Enqueue(like)
	drain(t, w)

	appends, invalidated := store.snapshot()
	if len(appends) != 0 || len(invalidated) != 0 {
		t.Fatalf("wrote for a like event: appends=%v invalidated=%v", appends, invalidated)
	}
}

func TestFollowWriter_DropsRatherThanBlockingWhenFull(t *testing.T) {
	// Follow writes must never apply backpressure to like ingestion: a stalled
	// Firestore would otherwise back up the whole jetstream loop.
	store := &fakeFollowStore{}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// No Run() goroutine, so nothing drains the buffer.
	w := NewFollowWriter(store, tracked, common.NewLogger(false), 1)

	if !w.Enqueue(makeFollow("did:plc:ours", "did:plc:a")) {
		t.Fatal("first enqueue should be accepted")
	}
	accepted := w.Enqueue(makeFollow("did:plc:ours", "did:plc:b"))
	if accepted {
		t.Fatal("enqueue past the buffer should drop, not block")
	}
	if w.Dropped() != 1 {
		t.Fatalf("Dropped() = %d, want 1", w.Dropped())
	}
}

func TestFollowWriter_StoreFailureIsNotFatal(t *testing.T) {
	store := &fakeFollowStore{appendErr: errors.New("firestore down")}
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	w := newTestWriter(store, tracked)

	w.Enqueue(makeFollow("did:plc:ours", "did:plc:a"))
	// The TTL refresh on the API side is the backstop; losing a delta is survivable.
	drain(t, w)
}

func TestTrackedUsers_KeepsThePreviousSetWhenRefreshFails(t *testing.T) {
	lister := &fakeUserLister{dids: []string{"did:plc:ours"}}
	tracked := NewTrackedUsers(lister, common.NewLogger(false))
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	lister.err = errors.New("firestore down")
	if err := tracked.Refresh(context.Background()); err == nil {
		t.Fatal("expected refresh to report the error")
	}

	// Dropping the set on a transient error would silently stop tracking
	// every user until the next successful refresh.
	if !tracked.Contains("ours") {
		t.Fatal("tracked set was cleared by a failed refresh")
	}
}

func TestTrackedUsers_EmptyBeforeFirstRefresh(t *testing.T) {
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, common.NewLogger(false))
	if tracked.Contains("ours") {
		t.Fatal("tracked set should be empty until the first refresh")
	}
}

func TestUserDocID(t *testing.T) {
	// Must match app.lib.firestore.user_doc_id in the API, or the writer
	// updates documents nobody reads.
	tests := map[string]string{
		"did:plc:abc123":   "abc123",
		"did:web:examp.le": "did:web:examp.le",
		"":                 "",
	}
	for did, want := range tests {
		if got := common.UserDocID(did); got != want {
			t.Errorf("UserDocID(%q) = %q, want %q", did, got, want)
		}
	}
}

// recordingLogger captures Metric() calls so the writer's counters can be
// asserted on. Without these exported, a Firestore that starts rejecting
// writes shows up only as log lines while the api side still looks healthy.
type recordingCollector struct {
	mu      sync.Mutex
	metrics map[string]float64
}

func (c *recordingCollector) Record(name string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metrics == nil {
		c.metrics = map[string]float64{}
	}
	c.metrics[name] += value
}

func (c *recordingCollector) get(name string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metrics[name]
}

func loggerWithMetrics(c *recordingCollector) *common.IngestLogger {
	// Metric() is a no-op on a disabled logger, so metrics need an enabled
	// one; the output goes nowhere to keep test runs quiet.
	l := common.NewLogger(true)
	l.SetOutput(io.Discard)
	l.SetMetricCollector(c)
	return l
}

func trackedOurs(t *testing.T, logger *common.IngestLogger) *TrackedUsers {
	t.Helper()
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:ours"}}, logger)
	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return tracked
}

func TestFollowWriter_CountsSuccessfulWrites(t *testing.T) {
	collector := &recordingCollector{}
	logger := loggerWithMetrics(collector)
	store := &fakeFollowStore{}
	w := NewFollowWriter(store, trackedOurs(t, logger), logger, 16)
	go w.Run(context.Background())

	w.Enqueue(makeFollow("did:plc:ours", "did:plc:a"))
	w.Enqueue(makeUnfollow("did:plc:ours"))
	drain(t, w)

	if got := collector.get("jetstream.follow_writes_count"); got != 2 {
		t.Fatalf("follow_writes_count = %v, want 2", got)
	}
}

func TestFollowWriter_CountsWriteFailures(t *testing.T) {
	collector := &recordingCollector{}
	logger := loggerWithMetrics(collector)
	store := &fakeFollowStore{appendErr: errors.New("firestore down")}
	w := NewFollowWriter(store, trackedOurs(t, logger), logger, 16)
	go w.Run(context.Background())

	w.Enqueue(makeFollow("did:plc:ours", "did:plc:a"))
	drain(t, w)

	if got := collector.get("jetstream.follow_write_failures_count"); got != 1 {
		t.Fatalf("follow_write_failures_count = %v, want 1", got)
	}
	if got := collector.get("jetstream.follow_writes_count"); got != 0 {
		t.Fatalf("follow_writes_count = %v, want 0 on failure", got)
	}
}

func TestFollowWriter_CountsDrops(t *testing.T) {
	collector := &recordingCollector{}
	logger := loggerWithMetrics(collector)
	// No Run() goroutine, so the buffer fills.
	w := NewFollowWriter(&fakeFollowStore{}, trackedOurs(t, logger), logger, 1)

	w.Enqueue(makeFollow("did:plc:ours", "did:plc:a"))
	w.Enqueue(makeFollow("did:plc:ours", "did:plc:b"))

	if got := collector.get("jetstream.follow_dropped_count"); got != 1 {
		t.Fatalf("follow_dropped_count = %v, want 1", got)
	}
}

func TestTrackedUsers_ReportsPopulationAsAGauge(t *testing.T) {
	collector := &recordingCollector{}
	logger := loggerWithMetrics(collector)
	tracked := NewTrackedUsers(&fakeUserLister{dids: []string{"did:plc:a", "did:plc:b"}}, logger)

	if err := tracked.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if got := collector.get("jetstream.tracked_users_rate"); got != 2 {
		t.Fatalf("tracked_users_rate = %v, want 2", got)
	}
}

func TestTrackedUsers_CountsRefreshFailures(t *testing.T) {
	collector := &recordingCollector{}
	logger := loggerWithMetrics(collector)
	lister := &fakeUserLister{err: errors.New("firestore down")}
	tracked := NewTrackedUsers(lister, logger)

	_ = tracked.Refresh(context.Background())

	if got := collector.get("jetstream.tracked_users_refresh_failures_count"); got != 1 {
		t.Fatalf("tracked_users_refresh_failures_count = %v, want 1", got)
	}
}
