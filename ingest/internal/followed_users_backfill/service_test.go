package followed_users_backfill

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

func ptrTime(t time.Time) *time.Time { return &t }

func TestNeedsRefresh_MissingEntry(t *testing.T) {
	reason, needs := NeedsRefresh(nil, time.Hour, 500)
	if !needs || reason != "missing" {
		t.Errorf("got reason=%q needs=%v", reason, needs)
	}
}

func TestNeedsRefresh_Incomplete(t *testing.T) {
	entry := &common.CacheEntry{Complete: false, GeneratedAt: ptrTime(time.Now())}
	reason, needs := NeedsRefresh(entry, time.Hour, 500)
	if !needs || reason != "incomplete" {
		t.Errorf("got reason=%q needs=%v", reason, needs)
	}
}

func TestNeedsRefresh_Invalidated(t *testing.T) {
	entry := &common.CacheEntry{Complete: true, GeneratedAt: ptrTime(time.Now()), InvalidatedAt: ptrTime(time.Now())}
	reason, needs := NeedsRefresh(entry, time.Hour, 500)
	if !needs || reason != "invalidated" {
		t.Errorf("got reason=%q needs=%v", reason, needs)
	}
}

func TestNeedsRefresh_PendingOverflow(t *testing.T) {
	pending := make([]string, 501)
	entry := &common.CacheEntry{Complete: true, GeneratedAt: ptrTime(time.Now()), PendingAdds: pending}
	reason, needs := NeedsRefresh(entry, time.Hour, 500)
	if !needs || reason != "pending_overflow" {
		t.Errorf("got reason=%q needs=%v", reason, needs)
	}
}

func TestNeedsRefresh_Stale(t *testing.T) {
	old := time.Now().Add(-2 * time.Hour)
	entry := &common.CacheEntry{Complete: true, GeneratedAt: &old}
	reason, needs := NeedsRefresh(entry, time.Hour, 500)
	if !needs || reason != "stale" {
		t.Errorf("got reason=%q needs=%v", reason, needs)
	}
}

func TestNeedsRefresh_Fresh(t *testing.T) {
	recent := time.Now().Add(-time.Minute)
	entry := &common.CacheEntry{Complete: true, GeneratedAt: &recent}
	reason, needs := NeedsRefresh(entry, time.Hour, 500)
	if needs {
		t.Errorf("expected fresh entry to not need refresh, got reason=%q", reason)
	}
}

type fakeLister struct{ dids []string }

func (f *fakeLister) ListUserDIDs(ctx context.Context) ([]string, error) { return f.dids, nil }

// fakeStore emulates common.FirestoreFollowStore. Guarded by a mutex because
// Service.Run's worker pool calls ReadEntry/WriteFollows from multiple
// goroutines concurrently — the real Firestore client is safe for that, so
// this fake must be too, or -race flags this test double rather than
// production code.
type fakeStore struct {
	mu      sync.Mutex
	entries map[string]*common.CacheEntry
	written map[string][]string

	// Targeted-query fakes: tests seed these directly rather than deriving
	// them from `entries`, since the real Firestore queries are independent
	// per-field filters, not a computation over the cache-entry struct.
	incompleteDocIDs  []string
	invalidatedDocIDs []string
	staleDocIDs       []string
	queryErr          error

	// userDIDs maps a followed-users-cache document ID to the real Bluesky
	// DID LookupUserDID should resolve it to; lookupErrFor triggers an error
	// for one specific docID so tests can exercise the "one bad lookup
	// doesn't abort the run" behavior.
	userDIDs     map[string]string
	lookupErrFor map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		entries:      map[string]*common.CacheEntry{},
		written:      map[string][]string{},
		userDIDs:     map[string]string{},
		lookupErrFor: map[string]bool{},
	}
}

func (s *fakeStore) QueryIncompleteDocIDs(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.incompleteDocIDs, s.queryErr
}

func (s *fakeStore) QueryInvalidatedDocIDs(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalidatedDocIDs, s.queryErr
}

func (s *fakeStore) QueryStaleDocIDs(ctx context.Context, cutoff time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.staleDocIDs, s.queryErr
}

func (s *fakeStore) LookupUserDID(ctx context.Context, docID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErrFor[docID] {
		return "", fmt.Errorf("lookup failed for %s", docID)
	}
	if did, ok := s.userDIDs[docID]; ok {
		return did, nil
	}
	return docID, nil
}

func (s *fakeStore) ReadEntry(ctx context.Context, userDocID string) (*common.CacheEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[userDocID], nil
}

func (s *fakeStore) WriteFollows(ctx context.Context, userDocID string, follows []string, complete bool, retentionDays int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.written[userDocID] = follows
	s.entries[userDocID] = &common.CacheEntry{Follows: follows, Complete: complete, GeneratedAt: ptrTime(time.Now())}
	return nil
}

// fakeFetcher stands in for BskyClient in Service tests — Service depends on
// an interface (see service.go) so it can be exercised without real HTTP.
type fakeFetcher struct {
	mu         sync.Mutex
	result     FollowsResult
	err        error
	calledWith []string
}

func (f *fakeFetcher) FetchFollows(ctx context.Context, actorDID string, limit int) (FollowsResult, error) {
	f.mu.Lock()
	f.calledWith = append(f.calledWith, actorDID)
	f.mu.Unlock()
	return f.result, f.err
}

func TestService_Run_RefreshesMissingAndStaleSkipsFresh(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:new", "did:plc:fresh"}}
	store := newFakeStore()
	recent := time.Now().Add(-time.Minute)
	store.entries["fresh"] = &common.CacheEntry{Complete: true, GeneratedAt: &recent}
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 2,
	})

	processed, refreshed, skipped, failed, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if processed != 2 || refreshed != 1 || skipped != 1 || failed != 0 {
		t.Errorf("got processed=%d refreshed=%d skipped=%d failed=%d", processed, refreshed, skipped, failed)
	}
	if _, ok := store.written["new"]; !ok {
		t.Error("expected the missing entry to be written")
	}
	if _, ok := store.written["fresh"]; ok {
		t.Error("expected the fresh entry to be left alone")
	}
}

// TestService_Run_CallsFetcherWithTheRealDIDNotTheFirestoreDocID guards
// against the bug found in the final whole-branch review: processOne must
// pass the fetcher the actual DID it got from the lister (e.g.
// "did:plc:user"), never a bare Firestore-document-ID-shaped string (e.g.
// "user") — that's the value common.FirestoreFollowStore.ListUserDIDs used
// to return before it was fixed to project user_did, and Bluesky's public
// API rejects a bare Firestore key with a 400.
func TestService_Run_CallsFetcherWithTheRealDIDNotTheFirestoreDocID(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore()
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	if _, _, _, _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if len(fetcher.calledWith) != 1 || fetcher.calledWith[0] != "did:plc:user" {
		t.Errorf("expected fetcher to be called with the real DID %q, got %v", "did:plc:user", fetcher.calledWith)
	}
}

// TestService_Run_ReturnsErrorWhenContextCancelledMidRun guards against the
// bug found in the final whole-branch review: Run must not report success
// (nil error) when ctx is cancelled before every user was processed —
// otherwise a partial run (e.g. from SIGTERM or a Cloud Run task timeout) is
// indistinguishable from a complete one to the caller.
func TestService_Run_ReturnsErrorWhenContextCancelledMidRun(t *testing.T) {
	dids := make([]string, 50)
	for i := range dids {
		dids[i] = "did:plc:user"
	}
	lister := &fakeLister{dids: dids}
	store := newFakeStore()

	ctx, cancel := context.WithCancel(context.Background())
	// blockingFetcher cancels ctx on its very first call, then blocks briefly
	// so the cancellation has time to cut the producer off before all 50
	// users are enqueued/processed.
	fetcher := &blockingFetcher{
		cancel: cancel,
		result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true},
	}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	processed, _, _, _, err := svc.Run(ctx)
	if err == nil {
		t.Fatal("expected Run to return an error when ctx is cancelled mid-run, got nil")
	}
	if processed >= len(dids) {
		t.Errorf("expected a partial run (processed < %d), got processed=%d", len(dids), processed)
	}
}

// blockingFetcher cancels the given context on its first call (simulating a
// SIGTERM/deadline arriving mid-run), then returns normally on every call —
// including the first, so the first user still counts as processed.
type blockingFetcher struct {
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
	result FollowsResult
}

func (f *blockingFetcher) FetchFollows(ctx context.Context, actorDID string, limit int) (FollowsResult, error) {
	f.mu.Lock()
	f.calls++
	first := f.calls == 1
	f.mu.Unlock()
	if first {
		f.cancel()
		time.Sleep(20 * time.Millisecond) // give the producer goroutine time to observe cancellation
	}
	return f.result, nil
}

func TestService_Run_ClampsNonPositiveConcurrencyToOne(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore()
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 0,
	})

	processed, refreshed, _, _, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if processed != 1 || refreshed != 1 {
		t.Errorf("expected Concurrency<=0 to be clamped to 1 worker and still process the user, got processed=%d refreshed=%d", processed, refreshed)
	}
}

func TestService_Run_RejectsNonPositiveMaxFollowedUsers(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore()
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 0,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	_, _, _, _, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to reject MaxFollowedUsers<=0 with an error")
	}
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if len(fetcher.calledWith) != 0 {
		t.Errorf("expected Run to fail before calling the fetcher at all, got calls: %v", fetcher.calledWith)
	}
}

// fakeMetricCollector implements common.MetricCollector so tests can observe
// Service.Metric calls directly. common's own test file defines an
// equivalent (mockMetricCollector) but it's unexported in package common, so
// it can't be reused from here.
type fakeMetricCollector struct {
	records map[string][]float64
}

func newFakeMetricCollector() *fakeMetricCollector {
	return &fakeMetricCollector{records: map[string][]float64{}}
}

func (f *fakeMetricCollector) Record(name string, value float64) {
	f.records[name] = append(f.records[name], value)
}

func TestService_Run_RecordsDriftBetweenServedAndRefreshedSets(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore()
	old := time.Now().Add(-2 * time.Hour)
	store.entries["user"] = &common.CacheEntry{
		Follows:     []string{"did:plc:a", "did:plc:b"},
		PendingAdds: []string{"did:plc:c"}, // already being served, merged
		Complete:    true,
		GeneratedAt: &old,
	}
	// Fresh walk finds a and d — b and c are "removed" relative to what was served, d is "added"
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:a", "did:plc:d"}, Complete: true}}

	logger := common.NewLogger(true)
	mc := newFakeMetricCollector()
	logger.SetMetricCollector(mc)

	svc := NewService(fetcher, store, lister, logger, ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	_, refreshed, _, _, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if refreshed != 1 {
		t.Fatalf("expected 1 refresh, got %d", refreshed)
	}

	added := mc.records["followed_users_backfill.refresh_drift_added_rate"]
	if len(added) != 1 || added[0] != 1 {
		t.Errorf("expected refresh_drift_added_rate=[1], got %v", added)
	}
	removed := mc.records["followed_users_backfill.refresh_drift_removed_rate"]
	if len(removed) != 1 || removed[0] != 2 {
		t.Errorf("expected refresh_drift_removed_rate=[2], got %v", removed)
	}
}

func TestService_Run_NoDriftMetricOnFirstEverWrite(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore() // no pre-existing entry for "user" — entry will be nil
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:a"}, Complete: true}}

	logger := common.NewLogger(true)
	mc := newFakeMetricCollector()
	logger.SetMetricCollector(mc)

	svc := NewService(fetcher, store, lister, logger, ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	_, refreshed, _, _, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if refreshed != 1 {
		t.Fatalf("expected 1 refresh, got %d", refreshed)
	}

	if got, ok := mc.records["followed_users_backfill.refresh_drift_added_rate"]; ok {
		t.Errorf("expected no refresh_drift_added_rate metric on first-ever write, got %v", got)
	}
	if got, ok := mc.records["followed_users_backfill.refresh_drift_removed_rate"]; ok {
		t.Errorf("expected no refresh_drift_removed_rate metric on first-ever write, got %v", got)
	}
}

func TestService_Run_NeverShrinksACompleteEntryWithAPartialWalk(t *testing.T) {
	lister := &fakeLister{dids: []string{"did:plc:user"}}
	store := newFakeStore()
	old := time.Now().Add(-2 * time.Hour)
	store.entries["user"] = &common.CacheEntry{
		Follows: []string{"did:plc:a", "did:plc:b"}, Complete: true, GeneratedAt: &old,
	}
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:a"}, Complete: false}}

	svc := NewService(fetcher, store, lister, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 1,
	})

	_, _, _, failed, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if failed != 1 {
		t.Errorf("expected the discarded partial to count as failed, got %d", failed)
	}
	if got := store.entries["user"].Follows; len(got) != 2 {
		t.Errorf("expected the complete entry to survive untouched, got %v", got)
	}
}

func TestService_RunTargeted_OnlyRefreshesEntriesMatchingATargetedCondition(t *testing.T) {
	store := newFakeStore()
	store.incompleteDocIDs = []string{"incomplete-user"}
	store.userDIDs["incomplete-user"] = "did:plc:incomplete-user"
	// A doc that would be "fresh" under NeedsRefresh is never returned by any
	// of the three targeted queries, so RunTargeted must not touch it —
	// mirrored here by simply not including it in any query result and
	// confirming it's absent from what gets written.
	store.entries["untouched-fresh-user"] = &common.CacheEntry{
		Complete: true, GeneratedAt: ptrTime(time.Now().Add(-time.Minute)),
	}
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, &fakeLister{}, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 2,
	})

	processed, refreshed, skipped, failed, err := svc.RunTargeted(context.Background())
	if err != nil {
		t.Fatalf("RunTargeted: %v", err)
	}
	if processed != 1 || refreshed != 1 || skipped != 0 || failed != 0 {
		t.Errorf("got processed=%d refreshed=%d skipped=%d failed=%d", processed, refreshed, skipped, failed)
	}
	if _, ok := store.written["incomplete-user"]; !ok {
		t.Error("expected the incomplete entry to be refreshed")
	}
	if _, ok := store.written["untouched-fresh-user"]; ok {
		t.Error("expected the entry never matching a targeted query to be left alone")
	}
}

func TestService_RunTargeted_LookupFailureCountsAsFailedButDoesNotAbortRun(t *testing.T) {
	store := newFakeStore()
	store.incompleteDocIDs = []string{"bad-lookup-user", "good-user"}
	store.userDIDs["good-user"] = "did:plc:good-user"
	store.lookupErrFor["bad-lookup-user"] = true
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, &fakeLister{}, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 2,
	})

	processed, refreshed, _, failed, err := svc.RunTargeted(context.Background())
	if err != nil {
		t.Fatalf("RunTargeted: %v", err)
	}
	if processed != 2 {
		t.Errorf("expected both candidates counted as processed, got %d", processed)
	}
	if refreshed != 1 {
		t.Errorf("expected the resolvable candidate to be refreshed, got %d", refreshed)
	}
	if failed != 1 {
		t.Errorf("expected the lookup failure to count as failed, got %d", failed)
	}
	if _, ok := store.written["good-user"]; !ok {
		t.Error("expected the resolvable candidate to still be processed")
	}
}

func TestService_RunTargeted_DedupesAnEntryMatchingMultipleConditions(t *testing.T) {
	store := newFakeStore()
	// "both-user" matches both incomplete and stale; it must only be
	// processed (and its fetcher called) once.
	store.incompleteDocIDs = []string{"both-user"}
	store.staleDocIDs = []string{"both-user"}
	store.userDIDs["both-user"] = "did:plc:both-user"
	fetcher := &fakeFetcher{result: FollowsResult{DIDs: []string{"did:plc:x"}, Complete: true}}

	svc := NewService(fetcher, store, &fakeLister{}, common.NewLogger(false), ServiceConfig{
		TTL: time.Hour, MaxPendingAdds: 500, MaxFollowedUsers: 1000,
		RetentionDays: 30, PerUserTimeout: time.Second, Concurrency: 2,
	})

	processed, refreshed, _, _, err := svc.RunTargeted(context.Background())
	if err != nil {
		t.Fatalf("RunTargeted: %v", err)
	}
	if processed != 1 || refreshed != 1 {
		t.Errorf("expected the duplicate candidate to be processed exactly once, got processed=%d refreshed=%d", processed, refreshed)
	}
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if len(fetcher.calledWith) != 1 {
		t.Errorf("expected the fetcher to be called exactly once for the deduped candidate, got %v", fetcher.calledWith)
	}
}
