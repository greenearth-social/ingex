package jetstream_ingest

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

type fakeHistoryStore struct {
	mu     sync.Mutex
	marked []string
	err    error
}

func (f *fakeHistoryStore) MarkHistoryStale(ctx context.Context, did string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.marked = append(f.marked, did)
	return nil
}

func (f *fakeHistoryStore) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	marked := f.marked
	f.marked = nil
	sort.Strings(marked)
	return marked
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestInvalidator(t *testing.T, store historyStore, tracked ...string) (*HistoryInvalidator, *fakeClock) {
	t.Helper()
	users := NewTrackedUsers(&fakeUserLister{dids: tracked}, common.NewLogger(false))
	if err := users.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	h := NewHistoryInvalidator(store, users, common.NewLogger(false), 40*time.Second, 120*time.Second)
	clock := &fakeClock{t: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
	h.now = clock.now
	return h, clock
}

func likesBy(dids ...string) []common.LikeDoc {
	likes := make([]common.LikeDoc, len(dids))
	for i, did := range dids {
		likes[i] = common.LikeDoc{AuthorDID: did}
	}
	return likes
}

func TestHistoryInvalidator_MarksTrackedUsersOnceTheirLikeIsSearchable(t *testing.T) {
	store := &fakeHistoryStore{}
	h, clock := newTestInvalidator(t, store, "did:plc:ours")

	h.NoteIndexed(likesBy("did:plc:ours", "did:plc:stranger", "did:plc:ours"))
	clock.advance(39 * time.Second)
	h.flush(context.Background())
	if got := store.take(); len(got) != 0 {
		t.Fatalf("marked %v before the like could be searchable", got)
	}

	clock.advance(time.Second)
	h.flush(context.Background())
	if got := store.take(); len(got) != 1 || got[0] != "did:plc:ours" {
		t.Fatalf("marked %v, want [did:plc:ours]", got)
	}
	if h.Pending() != 0 {
		t.Fatalf("pending = %d after marking, want 0", h.Pending())
	}
}

func TestHistoryInvalidator_WaitsForTheLatestLike(t *testing.T) {
	// A refresh that runs before a like is searchable rebuilds the history
	// without it, so a later like pushes the mark back.
	store := &fakeHistoryStore{}
	h, clock := newTestInvalidator(t, store, "did:plc:ours")

	h.NoteIndexed(likesBy("did:plc:ours"))
	clock.advance(30 * time.Second)
	h.NoteIndexed(likesBy("did:plc:ours"))
	clock.advance(20 * time.Second) // 50 s after the first like, 20 s after the second
	h.flush(context.Background())
	if got := store.take(); len(got) != 0 {
		t.Fatalf("marked %v before the second like was searchable", got)
	}

	clock.advance(20 * time.Second)
	h.flush(context.Background())
	if got := store.take(); len(got) != 1 {
		t.Fatalf("marked %v, want one mark", got)
	}
}

func TestHistoryInvalidator_ContinuousLikerIsMarkedAtMaxWaitAndAgainAfter(t *testing.T) {
	store := &fakeHistoryStore{}
	h, clock := newTestInvalidator(t, store, "did:plc:ours")

	for elapsed := time.Duration(0); elapsed <= 120*time.Second; elapsed += 10 * time.Second {
		h.NoteIndexed(likesBy("did:plc:ours"))
		h.flush(context.Background())
		if elapsed < 120*time.Second {
			if got := store.take(); len(got) != 0 {
				t.Fatalf("marked %v at %s, before max wait", got, elapsed)
			}
		}
		clock.advance(10 * time.Second)
	}
	// Max wait reached on the last iteration: one mark, and a follow-up kept
	// for the like that is not searchable yet.
	if got := store.take(); len(got) != 1 {
		t.Fatalf("marked %v at max wait, want one mark", got)
	}
	if h.Pending() != 1 {
		t.Fatalf("pending = %d, want a follow-up mark", h.Pending())
	}

	clock.advance(40 * time.Second)
	h.flush(context.Background())
	if got := store.take(); len(got) != 1 {
		t.Fatalf("marked %v after the last like was searchable, want one mark", got)
	}
	if h.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", h.Pending())
	}
}

func TestHistoryInvalidator_FailedMarkIsNotRetried(t *testing.T) {
	// A failure is logged and counted; the API's maximum history age bounds
	// the staleness, and retrying could hammer an unavailable Firestore.
	store := &fakeHistoryStore{err: errors.New("unavailable")}
	h, clock := newTestInvalidator(t, store, "did:plc:ours")

	h.NoteIndexed(likesBy("did:plc:ours"))
	clock.advance(time.Minute)
	h.flush(context.Background())

	if h.Pending() != 0 {
		t.Fatalf("pending = %d after a failed mark, want 0", h.Pending())
	}
}

func TestHistoryInvalidator_NilIsANoOp(t *testing.T) {
	var h *HistoryInvalidator
	h.NoteIndexed(likesBy("did:plc:ours"))
}

func TestUserHistoryCacheDocIDMatchesTheAPI(t *testing.T) {
	// hashlib.sha256(b"did:plc:abc").hexdigest(), as the API keys the document.
	const want = "b0b204c271d7714364fa3d9da802b08106ee8a318eeb4567a21e95602ddefa39"
	if got := common.UserHistoryCacheDocID("did:plc:abc"); got != want {
		t.Fatalf("UserHistoryCacheDocID = %q, want %q", got, want)
	}
}
