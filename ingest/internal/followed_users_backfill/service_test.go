package followed_users_backfill

import (
	"context"
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
}

func newFakeStore() *fakeStore {
	return &fakeStore{entries: map[string]*common.CacheEntry{}, written: map[string][]string{}}
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
