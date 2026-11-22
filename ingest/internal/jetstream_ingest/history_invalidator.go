package jetstream_ingest

import (
	"context"
	"sync"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// The API caches each user's recent like history, which feeds both its
// two-tower retrieval and its heavy ranker, and used to rebuild it from
// Elasticsearch every 10 minutes whether or not the user had liked anything.
// For the users we track (the same set as follow deltas), jetstream tells it
// instead: after a tracked user's like is indexed, their cached history is
// marked stale, so the API can keep it for hours otherwise (api#602).
//
// A newly indexed like is not searchable until the likes index refreshes
// (30 s), and a refresh the API runs before then would rebuild the history
// without it and look current. So a user is marked only once `delay` has
// passed since their latest indexed like. A user who keeps liking is still
// marked every `maxWait`, and once more after their last like is searchable.

// historyStore is the Firestore surface the invalidator needs.
type historyStore interface {
	MarkHistoryStale(ctx context.Context, did string) error
}

type pendingInvalidation struct {
	first  time.Time // when this pending mark was opened
	latest time.Time // when the latest like it covers was indexed
}

// HistoryInvalidator coalesces tracked users' indexed likes into history marks.
type HistoryInvalidator struct {
	store   historyStore
	tracked *TrackedUsers
	logger  *common.IngestLogger
	delay   time.Duration
	maxWait time.Duration
	now     func() time.Time

	mu      sync.Mutex
	pending map[string]pendingInvalidation
}

// NewHistoryInvalidator returns an invalidator for tracked users' like history.
func NewHistoryInvalidator(store historyStore, tracked *TrackedUsers, logger *common.IngestLogger, delay, maxWait time.Duration) *HistoryInvalidator {
	if maxWait < delay {
		maxWait = delay
	}
	return &HistoryInvalidator{
		store:   store,
		tracked: tracked,
		logger:  logger,
		delay:   delay,
		maxWait: maxWait,
		now:     time.Now,
		pending: make(map[string]pendingInvalidation),
	}
}

// NoteIndexed records likes that were just indexed. Never blocks on Firestore.
func (h *HistoryInvalidator) NoteIndexed(likes []common.LikeDoc) {
	if h == nil || len(likes) == 0 {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, like := range likes {
		did := like.AuthorDID
		if did == "" || !h.tracked.Contains(common.UserDocID(did)) {
			continue
		}
		if p, ok := h.pending[did]; ok {
			p.latest = now
			h.pending[did] = p
		} else {
			h.pending[did] = pendingInvalidation{first: now, latest: now}
		}
	}
}

// due returns the users to mark now, keeping a follow-up mark for any whose
// latest like is not searchable yet.
func (h *HistoryInvalidator) due() []string {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	var dids []string
	for did, p := range h.pending {
		searchable := !now.Before(p.latest.Add(h.delay))
		if !searchable && now.Before(p.first.Add(h.maxWait)) {
			continue
		}
		dids = append(dids, did)
		if searchable {
			delete(h.pending, did)
		} else {
			h.pending[did] = pendingInvalidation{first: now, latest: p.latest}
		}
	}
	return dids
}

// Pending returns how many users are waiting to be marked.
func (h *HistoryInvalidator) Pending() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pending)
}

// Run writes due marks every interval until ctx is cancelled. Marks still
// pending at shutdown are dropped: the API's maximum history age bounds how
// stale that can leave a history.
func (h *HistoryInvalidator) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.flush(ctx)
		}
	}
}

func (h *HistoryInvalidator) flush(ctx context.Context) {
	for _, did := range h.due() {
		if err := h.store.MarkHistoryStale(ctx, did); err != nil {
			h.logger.Error("Failed to mark like history stale for %s: %v", did, err)
			h.logger.Metric("jetstream.history_invalidation_failures_count", 1)
			continue
		}
		h.logger.Metric("jetstream.history_invalidations_count", 1)
	}
}
