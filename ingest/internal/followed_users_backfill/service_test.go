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
	result FollowsResult
	err    error
}

func (f *fakeFetcher) FetchFollows(ctx context.Context, actorDID string, limit int) (FollowsResult, error) {
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
