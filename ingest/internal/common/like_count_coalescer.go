package common

import (
	"container/list"
	"sync"
)

// LikeCountCoalescer decides which like-count deltas to write now and which to
// hold for the next periodic flush.
//
// Every like-count update re-indexes the whole post document, including its
// HNSW vectors, so writing each like as it arrives is the dominant cost of like
// ingestion. Popular posts are liked often and their counts tolerate a few
// minutes of staleness; posts with only a handful of likes are where an
// off-by-a-few count matters (and where the quality threshold is crossed). So:
//
//   - A post whose last known count is below exactBelow, or whose count isn't
//     known yet, is written immediately — along with anything held for it.
//   - Everything else is held and summed per post until Drain.
//
// Known counts come from the bulk update responses (Observe), held in a
// bounded LRU. Held deltas are in memory only: a restart loses up to one flush
// interval of likes on popular posts, which is an accepted inaccuracy.
//
// Safe for concurrent use.
type LikeCountCoalescer struct {
	mu         sync.Mutex
	exactBelow int
	known      *countLRU
	pending    map[string]int
}

// NewLikeCountCoalescer returns a coalescer that writes posts under exactBelow
// likes immediately and remembers the counts of up to cacheSize posts.
func NewLikeCountCoalescer(exactBelow, cacheSize int) *LikeCountCoalescer {
	return &LikeCountCoalescer{
		exactBelow: exactBelow,
		known:      newCountLRU(cacheSize),
		pending:    make(map[string]int),
	}
}

// Route returns the updates to write now, one per post with deltas summed,
// and holds the rest. A nil coalescer writes everything.
func (c *LikeCountCoalescer) Route(updates []LikeCountUpdate) []LikeCountUpdate {
	if c == nil {
		return updates
	}
	aggregated := aggregateLikeCountUpdates(updates)

	c.mu.Lock()
	defer c.mu.Unlock()
	var now []LikeCountUpdate
	for uri, increment := range aggregated {
		if count, ok := c.known.get(uri); ok && count >= c.exactBelow {
			if total := c.pending[uri] + increment; total != 0 {
				c.pending[uri] = total
			} else {
				delete(c.pending, uri) // a like and its unlike cancel out
			}
			continue
		}
		increment += c.pending[uri]
		delete(c.pending, uri)
		if increment != 0 {
			now = append(now, LikeCountUpdate{SubjectURI: uri, Increment: increment})
		}
	}
	return now
}

// Drain returns every held update and clears them.
func (c *LikeCountCoalescer) Drain() []LikeCountUpdate {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	drained := make([]LikeCountUpdate, 0, len(c.pending))
	for uri, increment := range c.pending {
		drained = append(drained, LikeCountUpdate{SubjectURI: uri, Increment: increment})
	}
	c.pending = make(map[string]int)
	return drained
}

// Observe records the post-update counts from a bulk update response.
func (c *LikeCountCoalescer) Observe(results []LikeCountResult) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range results {
		c.known.set(r.AtURI, r.LikeCount)
	}
}

// Stats reports how many posts have held deltas and how many counts are cached.
func (c *LikeCountCoalescer) Stats() (pending, cached int) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending), c.known.len()
}

// countLRU is a fixed-capacity map from post URI to its last known like count.
type countLRU struct {
	capacity int
	order    *list.List // front = most recently used
	items    map[string]*list.Element
}

type countEntry struct {
	uri   string
	count int
}

func newCountLRU(capacity int) *countLRU {
	return &countLRU{capacity: capacity, order: list.New(), items: make(map[string]*list.Element)}
}

func (l *countLRU) get(uri string) (int, bool) {
	el, ok := l.items[uri]
	if !ok {
		return 0, false
	}
	l.order.MoveToFront(el)
	return entryOf(el).count, true
}

func (l *countLRU) set(uri string, count int) {
	if el, ok := l.items[uri]; ok {
		entryOf(el).count = count
		l.order.MoveToFront(el)
		return
	}
	if l.capacity <= 0 {
		return
	}
	l.items[uri] = l.order.PushFront(&countEntry{uri: uri, count: count})
	if l.order.Len() > l.capacity {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.items, entryOf(oldest).uri)
	}
}

func (l *countLRU) len() int { return l.order.Len() }

// entryOf unwraps a list element; the list only ever holds *countEntry.
func entryOf(el *list.Element) *countEntry {
	entry, ok := el.Value.(*countEntry)
	if !ok {
		panic("countLRU: unexpected list element type")
	}
	return entry
}
