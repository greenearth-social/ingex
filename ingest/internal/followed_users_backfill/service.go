package followed_users_backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// NeedsRefresh reports whether entry needs a fresh Bluesky walk, and why.
// Ports api's FollowedUsersCache._staleness verbatim — the two must agree on
// what "stale" means even though only this job now acts on it.
func NeedsRefresh(entry *common.CacheEntry, ttl time.Duration, maxPendingAdds int) (string, bool) {
	if entry == nil {
		return "missing", true
	}
	if !entry.Complete {
		return "incomplete", true
	}
	if entry.InvalidatedAt != nil {
		return "invalidated", true
	}
	if len(entry.PendingAdds) > maxPendingAdds {
		return "pending_overflow", true
	}
	if entry.GeneratedAt == nil || time.Since(*entry.GeneratedAt) > ttl {
		return "stale", true
	}
	return "", false
}

// followFetcher is the subset of BskyClient the Service needs — an interface
// so tests substitute a fake instead of real HTTP.
type followFetcher interface {
	FetchFollows(ctx context.Context, actorDID string, limit int) (FollowsResult, error)
}

// followStore is the subset of common.FirestoreFollowStore the Service needs.
// The four Query*/Lookup methods are used only by RunTargeted, but live on
// this same interface rather than a second one: *common.FirestoreFollowStore
// already implements all of them together, RunTargeted needs both the
// per-user read/write methods (via the shared runOverUserDIDs core) and the
// targeted queries in the same call, and a single interface means the tests'
// existing fakeStore is extended in place rather than juggling two store
// values that must always alias the same underlying fake.
type followStore interface {
	ReadEntry(ctx context.Context, userDocID string) (*common.CacheEntry, error)
	WriteFollows(ctx context.Context, userDocID string, follows []string, complete bool, retentionDays int) error
	QueryIncompleteDocIDs(ctx context.Context) ([]string, error)
	QueryInvalidatedDocIDs(ctx context.Context) ([]string, error)
	QueryStaleDocIDs(ctx context.Context, cutoff time.Time) ([]string, error)
	LookupUserDID(ctx context.Context, docID string) (string, error)
}

// userLister enumerates the DIDs of every tracked user. common has no
// exported UserLister type of its own (the one interface with this shape
// lives unexported-adjacent in internal/jetstream_ingest); *common.FirestoreFollowStore
// already implements ListUserDIDs, so this local interface lets Task 5 pass
// the same store value for both followStore and userLister.
type userLister interface {
	ListUserDIDs(ctx context.Context) ([]string, error)
}

// ServiceConfig tunes the backfill job's staleness rule and worker pool.
type ServiceConfig struct {
	TTL              time.Duration
	MaxPendingAdds   int
	MaxFollowedUsers int
	RetentionDays    int
	PerUserTimeout   time.Duration
	Concurrency      int
}

// Service walks every tracked user's Bluesky follows on a schedule and
// writes the results to the followed-users cache.
type Service struct {
	fetcher followFetcher
	store   followStore
	lister  userLister
	logger  *common.IngestLogger
	cfg     ServiceConfig
}

// NewService returns a Service that walks follows via fetcher, reads/writes
// cache state via store, and enumerates tracked users via lister.
func NewService(fetcher followFetcher, store followStore, lister userLister, logger *common.IngestLogger, cfg ServiceConfig) *Service {
	return &Service{fetcher: fetcher, store: store, lister: lister, logger: logger, cfg: cfg}
}

type processOutcome struct{ refreshed, skipped, failed bool }

// Run walks every tracked user that needs a refresh and writes the result.
// Bounded worker pool: Concurrency goroutines, each processing one user's
// walk+write at a time (a user's DID never appears twice in one run, so no
// synchronization is needed beyond the counters below).
func (s *Service) Run(ctx context.Context) (processed, refreshed, skipped, failed int, err error) {
	if s.cfg.MaxFollowedUsers <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("MaxFollowedUsers must be positive, got %d", s.cfg.MaxFollowedUsers)
	}

	dids, err := s.lister.ListUserDIDs(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	s.logger.Metric("followed_users_backfill.total_users_rate", float64(len(dids)))

	return s.runOverUserDIDs(ctx, dids)
}

// RunTargeted walks only the users whose followed-users-cache entry is
// incomplete, invalidated, or past the TTL cutoff — cost scales with actual
// staleness volume rather than the full tracked-user population. Unlike Run
// it does not reject a fresh entry a second time (NeedsRefresh inside
// runOverUserDIDs still applies, but a doc that only matched one of the three
// query filters can never be "fresh").
//
// A LookupUserDID failure for one candidate is recorded as failed and does
// not abort the run; the DID is simply excluded from the walk since a walk
// needs the real Bluesky DID, not the Firestore document ID.
func (s *Service) RunTargeted(ctx context.Context) (processed, refreshed, skipped, failed int, err error) {
	if s.cfg.MaxFollowedUsers <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("MaxFollowedUsers must be positive, got %d", s.cfg.MaxFollowedUsers)
	}

	docIDs, err := s.collectTargetedDocIDs(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	userDIDs := make([]string, 0, len(docIDs))
	var lookupFailures int
	for _, docID := range docIDs {
		did, err := s.store.LookupUserDID(ctx, docID)
		if err != nil {
			s.logger.Error("Failed to resolve user_did for %s: %v", docID, err)
			lookupFailures++
			continue
		}
		userDIDs = append(userDIDs, did)
	}
	s.logger.Metric("followed_users_backfill.targeted_candidates_rate", float64(len(docIDs)))

	processed, refreshed, skipped, failed, err = s.runOverUserDIDs(ctx, userDIDs)
	processed += lookupFailures
	failed += lookupFailures
	return processed, refreshed, skipped, failed, err
}

// collectTargetedDocIDs unions the followed-users-cache document IDs
// matching any of the three targeted conditions, deduplicating an entry that
// matches more than one (e.g. both incomplete and past the stale cutoff) so
// it is only processed once.
func (s *Service) collectTargetedDocIDs(ctx context.Context) ([]string, error) {
	incomplete, err := s.store.QueryIncompleteDocIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying incomplete entries: %w", err)
	}
	invalidated, err := s.store.QueryInvalidatedDocIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying invalidated entries: %w", err)
	}
	cutoff := time.Now().Add(-s.cfg.TTL)
	stale, err := s.store.QueryStaleDocIDs(ctx, cutoff)
	if err != nil {
		return nil, fmt.Errorf("querying stale entries: %w", err)
	}

	seen := make(map[string]struct{})
	var union []string
	for _, ids := range [][]string{incomplete, invalidated, stale} {
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				union = append(union, id)
			}
		}
	}
	return union, nil
}

// runOverUserDIDs is the shared worker-pool core behind Run and RunTargeted:
// everything Run used to do after enumerating its user-DID list. Bounded
// worker pool: Concurrency goroutines, each processing one user's walk+write
// at a time (a user's DID never appears twice in one run, so no
// synchronization is needed beyond the counters below).
func (s *Service) runOverUserDIDs(ctx context.Context, dids []string) (processed, refreshed, skipped, failed int, err error) {
	concurrency := s.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	work := make(chan string)
	results := make(chan processOutcome)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for userDID := range work {
				results <- s.processOne(ctx, userDID)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	go func() {
		defer close(work)
		for _, did := range dids {
			select {
			case work <- did:
			case <-ctx.Done():
				return
			}
		}
	}()

	for r := range results {
		processed++
		switch {
		case r.refreshed:
			refreshed++
		case r.failed:
			failed++
		default:
			skipped++
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return processed, refreshed, skipped, failed, fmt.Errorf("run cancelled after processing %d/%d users: %w", processed, len(dids), ctxErr)
	}
	return processed, refreshed, skipped, failed, nil
}

func (s *Service) processOne(ctx context.Context, userDID string) processOutcome {
	userDocID := common.UserDocID(userDID)
	entry, err := s.store.ReadEntry(ctx, userDocID)
	if err != nil {
		s.logger.Error("Failed to read followed-users cache for %s: %v", userDocID, err)
		s.logger.Metric("followed_users_backfill.lookup_count.read_error", 1)
		return processOutcome{failed: true}
	}

	reason, needs := NeedsRefresh(entry, s.cfg.TTL, s.cfg.MaxPendingAdds)
	s.logger.Metric("followed_users_backfill.lookup_count."+stalenessOutcome(reason, needs), 1)
	if !needs {
		return processOutcome{skipped: true}
	}

	walkCtx, cancel := context.WithTimeout(ctx, s.cfg.PerUserTimeout)
	fetch, err := s.fetcher.FetchFollows(walkCtx, userDID, s.cfg.MaxFollowedUsers)
	cancel()
	if err != nil {
		s.logger.Error("Followed-users walk failed for %s: %v", userDocID, err)
		s.logger.Metric("followed_users_backfill.refresh_count.failed", 1)
		return processOutcome{failed: true}
	}

	if !fetch.Complete && entry != nil && entry.Complete {
		// A partial walk must never shrink a complete entry — ported from
		// api's FollowedUsersCache._refresh verbatim.
		s.logger.Info("Discarding partial followed-users walk for %s (%d dids); keeping the complete entry", userDocID, len(fetch.DIDs))
		s.logger.Metric("followed_users_backfill.refresh_count.partial_discarded", 1)
		return processOutcome{failed: true}
	}

	s.noteDrift(entry, fetch, userDocID)

	if err := s.store.WriteFollows(ctx, userDocID, fetch.DIDs, fetch.Complete, s.cfg.RetentionDays); err != nil {
		s.logger.Error("Failed to write followed-users cache for %s: %v", userDocID, err)
		s.logger.Metric("followed_users_backfill.refresh_count.write_error", 1)
		return processOutcome{failed: true}
	}

	outcome := "success"
	if !fetch.Complete {
		outcome = "partial"
	}
	s.logger.Metric("followed_users_backfill.refresh_count."+outcome, 1)
	return processOutcome{refreshed: true}
}

// noteDrift compares what was being served (entry.Follows ∪ entry.PendingAdds)
// against a fresh walk's result and records added/removed counts — ports
// api's now-deleted FollowedUsersCache._note_drift, "the only correctness
// signal in the system" for detecting silent jetstream drift. Only records
// when entry is non-nil (there was something previously served to drift
// from); a cold "missing" entry has nothing to compare against.
func (s *Service) noteDrift(entry *common.CacheEntry, fetch FollowsResult, userDocID string) {
	if entry == nil {
		return
	}
	served := make(map[string]struct{}, len(entry.Follows)+len(entry.PendingAdds))
	for _, did := range entry.Follows {
		served[did] = struct{}{}
	}
	for _, did := range entry.PendingAdds {
		served[did] = struct{}{}
	}
	refreshedSet := make(map[string]struct{}, len(fetch.DIDs))
	for _, did := range fetch.DIDs {
		refreshedSet[did] = struct{}{}
	}
	added, removed := 0, 0
	for did := range refreshedSet {
		if _, ok := served[did]; !ok {
			added++
		}
	}
	for did := range served {
		if _, ok := refreshedSet[did]; !ok {
			removed++
		}
	}
	if added > 0 || removed > 0 {
		s.logger.Info("Backfill refresh for %s moved the set: +%d -%d", userDocID, added, removed)
	}
	s.logger.Metric("followed_users_backfill.refresh_drift_added_rate", float64(added))
	s.logger.Metric("followed_users_backfill.refresh_drift_removed_rate", float64(removed))
}

func stalenessOutcome(reason string, needs bool) string {
	if !needs {
		return "hit"
	}
	return reason
}
