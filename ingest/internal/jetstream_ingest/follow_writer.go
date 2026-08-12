package jetstream_ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// The API keeps a per-user cache of Bluesky follow lists so its candidate
// generators stop walking app.bsky.graph.getFollows on every feed request
// (api#83). Jetstream is what keeps that cache current: we already consume the
// unfiltered firehose for likes, so follow events cost no extra bandwidth.
//
// Only our own users matter — a few hundred DIDs out of the whole network — so
// events are filtered against the users collection before any write. That is
// what keeps Firestore write volume negligible.
//
// Writes are deliberately narrow. A follow appends to pending_adds; an
// unfollow only stamps invalidated_at, because a jetstream delete carries just
// did/collection/rkey and there is no way to tell who was unfollowed. The API
// reconciles both on its next refresh.

// followStore is the Firestore surface the writer needs. An interface so the
// writer can be tested without a Firestore client.
type followStore interface {
	AppendPendingFollow(ctx context.Context, userDocID, subjectDID string) error
	InvalidateFollows(ctx context.Context, userDocID string) error
}

// UserLister enumerates the DIDs of users we serve feeds to.
type UserLister interface {
	ListUserDIDs(ctx context.Context) ([]string, error)
}

// TrackedUsers holds the set of user document IDs we write follow deltas for.
// Refreshed periodically; reads are lock-free on the hot path.
type TrackedUsers struct {
	lister UserLister
	logger *common.IngestLogger
	set    atomic.Value // map[string]struct{}
}

// NewTrackedUsers returns an empty tracked-user set; call Refresh or Run to populate it.
func NewTrackedUsers(lister UserLister, logger *common.IngestLogger) *TrackedUsers {
	t := &TrackedUsers{lister: lister, logger: logger}
	t.set.Store(map[string]struct{}{})
	return t
}

// Contains reports whether userDocID is one of ours.
func (t *TrackedUsers) Contains(userDocID string) bool {
	set, _ := t.set.Load().(map[string]struct{})
	_, ok := set[userDocID]
	return ok
}

// Len returns the number of tracked users.
func (t *TrackedUsers) Len() int {
	set, _ := t.set.Load().(map[string]struct{})
	return len(set)
}

// Refresh replaces the set from Firestore. On error the previous set is kept:
// dropping it would silently stop tracking every user until the next success.
func (t *TrackedUsers) Refresh(ctx context.Context) error {
	dids, err := t.lister.ListUserDIDs(ctx)
	if err != nil {
		t.logger.Error("Failed to refresh tracked users: %v", err)
		// A stuck set means follow deltas silently stop being written for
		// users added since the last success, so this needs to be visible.
		t.logger.Metric("jetstream.tracked_users_refresh_failures_count", 1)
		return err
	}
	set := make(map[string]struct{}, len(dids))
	for _, did := range dids {
		set[common.UserDocID(did)] = struct{}{}
	}
	t.set.Store(set)
	t.logger.Metric("jetstream.tracked_users_rate", float64(len(set)))
	return nil
}

// Run refreshes the set until ctx is cancelled, starting immediately.
func (t *TrackedUsers) Run(ctx context.Context, interval time.Duration) {
	if err := t.Refresh(ctx); err == nil {
		t.logger.Info("Tracking follow events for %d users", t.Len())
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = t.Refresh(ctx)
		}
	}
}

type followEventKind int

const (
	followCreate followEventKind = iota
	followDelete
)

type followEvent struct {
	kind       followEventKind
	userDocID  string
	subjectDID string
}

// FollowWriter applies follow deltas to the API's followed-users cache.
type FollowWriter struct {
	store   followStore
	tracked *TrackedUsers
	logger  *common.IngestLogger

	events    chan followEvent
	done      chan struct{}
	closeOnce sync.Once
	dropped   atomic.Int64
	written   atomic.Int64
}

// NewFollowWriter returns a writer buffering up to buffer pending deltas.
func NewFollowWriter(store followStore, tracked *TrackedUsers, logger *common.IngestLogger, buffer int) *FollowWriter {
	if buffer <= 0 {
		buffer = 1
	}
	return &FollowWriter{
		store:   store,
		tracked: tracked,
		logger:  logger,
		events:  make(chan followEvent, buffer),
		done:    make(chan struct{}),
	}
}

// Enqueue queues msg if it is a follow event from one of our users.
//
// Never blocks: it returns false and counts a drop when the buffer is full, so
// a slow or unavailable Firestore can never apply backpressure to the like
// ingestion path. Losing a delta is survivable — the API's TTL refresh
// reconciles it.
func (w *FollowWriter) Enqueue(msg common.JetstreamMessage) bool {
	var event followEvent
	switch {
	case msg.IsFollow():
		event = followEvent{
			kind:       followCreate,
			userDocID:  common.UserDocID(msg.GetAuthorDID()),
			subjectDID: msg.GetFollowSubjectDID(),
		}
	case msg.IsFollowDelete():
		event = followEvent{
			kind:      followDelete,
			userDocID: common.UserDocID(msg.GetAuthorDID()),
		}
	default:
		return false
	}

	if !w.tracked.Contains(event.userDocID) {
		return false
	}

	select {
	case w.events <- event:
		return true
	default:
		w.dropped.Add(1)
		w.logger.Error("Follow event buffer full; dropped event for %s", event.userDocID)
		w.logger.Metric("jetstream.follow_dropped_count", 1)
		return false
	}
}

// Run applies queued events until the writer is closed or ctx is cancelled.
func (w *FollowWriter) Run(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.events:
			if !ok {
				return
			}
			w.apply(ctx, event)
		}
	}
}

func (w *FollowWriter) apply(ctx context.Context, event followEvent) {
	var err error
	switch event.kind {
	case followCreate:
		err = w.store.AppendPendingFollow(ctx, event.userDocID, event.subjectDID)
	case followDelete:
		err = w.store.InvalidateFollows(ctx, event.userDocID)
	}
	if err != nil {
		// Never fatal: the API refreshes on a TTL regardless. Counted so a
		// Firestore that starts rejecting writes is visible as more than a
		// log line — the api side keeps looking healthy while its entries
		// quietly go stale.
		w.logger.Error("Failed to write follow delta for %s: %v", event.userDocID, err)
		w.logger.Metric("jetstream.follow_write_failures_count", 1)
		return
	}
	w.written.Add(1)
	w.logger.Metric("jetstream.follow_writes_count", 1)
}

// Close stops the writer after the queued events have been applied.
func (w *FollowWriter) Close() {
	w.closeOnce.Do(func() { close(w.events) })
}

// Done is closed once Run has returned.
func (w *FollowWriter) Done() <-chan struct{} { return w.done }

// Dropped counts events discarded because the buffer was full.
func (w *FollowWriter) Dropped() int64 { return w.dropped.Load() }

// Written counts deltas successfully applied.
func (w *FollowWriter) Written() int64 { return w.written.Load() }
