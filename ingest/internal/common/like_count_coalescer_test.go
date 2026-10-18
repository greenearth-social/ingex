package common

import (
	"sync"
	"testing"
)

func byURI(updates []LikeCountUpdate) map[string]int {
	out := make(map[string]int, len(updates))
	for _, u := range updates {
		if _, dup := out[u.SubjectURI]; dup {
			panic("duplicate update for " + u.SubjectURI)
		}
		out[u.SubjectURI] = u.Increment
	}
	return out
}

func TestCoalescer_UnknownPostsAreWrittenImmediately(t *testing.T) {
	c := NewLikeCountCoalescer(10, 100)

	now := byURI(c.Route([]LikeCountUpdate{
		{SubjectURI: "at://a", Increment: 1},
		{SubjectURI: "at://a", Increment: 1},
		{SubjectURI: "at://b", Increment: 1},
	}))

	if len(now) != 2 || now["at://a"] != 2 || now["at://b"] != 1 {
		t.Fatalf("expected both unknown posts written with summed deltas, got %v", now)
	}
	if pending, _ := c.Stats(); pending != 0 {
		t.Fatalf("expected nothing held, got %d", pending)
	}
}

func TestCoalescer_SmallPostsStayExact(t *testing.T) {
	c := NewLikeCountCoalescer(10, 100)
	c.Observe([]LikeCountResult{{AtURI: "at://small", LikeCount: 9}})

	now := byURI(c.Route([]LikeCountUpdate{{SubjectURI: "at://small", Increment: 1}}))

	if now["at://small"] != 1 {
		t.Fatalf("expected a post under the threshold to be written immediately, got %v", now)
	}
}

func TestCoalescer_PopularPostsAreHeldUntilDrain(t *testing.T) {
	c := NewLikeCountCoalescer(10, 100)
	c.Observe([]LikeCountResult{{AtURI: "at://popular", LikeCount: 10}})

	for i := 0; i < 3; i++ {
		if now := c.Route([]LikeCountUpdate{{SubjectURI: "at://popular", Increment: 1}}); len(now) != 0 {
			t.Fatalf("expected a post at the threshold to be held, got %v", now)
		}
	}
	c.Route([]LikeCountUpdate{{SubjectURI: "at://popular", Increment: -1}})

	drained := byURI(c.Drain())
	if len(drained) != 1 || drained["at://popular"] != 2 {
		t.Fatalf("expected one summed update (+3 -1), got %v", drained)
	}
	if again := c.Drain(); len(again) != 0 {
		t.Fatalf("expected drain to clear held updates, got %v", again)
	}
}

func TestCoalescer_CancellingDeltasAreDropped(t *testing.T) {
	c := NewLikeCountCoalescer(10, 100)
	c.Observe([]LikeCountResult{{AtURI: "at://p", LikeCount: 50}})

	c.Route([]LikeCountUpdate{{SubjectURI: "at://p", Increment: 1}})
	c.Route([]LikeCountUpdate{{SubjectURI: "at://p", Increment: -1}})

	if drained := c.Drain(); len(drained) != 0 {
		t.Fatalf("expected a like and its unlike to cancel, got %v", drained)
	}
}

func TestCoalescer_ImmediateWriteCarriesHeldDeltas(t *testing.T) {
	// A post can drop below the threshold (unlikes, or a cache refresh) while
	// it has deltas held; its next immediate write must include them.
	c := NewLikeCountCoalescer(10, 100)
	c.Observe([]LikeCountResult{{AtURI: "at://p", LikeCount: 12}})
	c.Route([]LikeCountUpdate{{SubjectURI: "at://p", Increment: 2}})
	c.Observe([]LikeCountResult{{AtURI: "at://p", LikeCount: 8}})

	now := byURI(c.Route([]LikeCountUpdate{{SubjectURI: "at://p", Increment: 1}}))

	if now["at://p"] != 3 {
		t.Fatalf("expected the held +2 folded into the immediate +1, got %v", now)
	}
	if drained := c.Drain(); len(drained) != 0 {
		t.Fatalf("expected nothing left held, got %v", drained)
	}
}

func TestCoalescer_EvictedPostsFallBackToImmediate(t *testing.T) {
	c := NewLikeCountCoalescer(10, 2)
	c.Observe([]LikeCountResult{
		{AtURI: "at://old", LikeCount: 100},
		{AtURI: "at://b", LikeCount: 100},
		{AtURI: "at://c", LikeCount: 100}, // evicts at://old
	})

	if _, cached := c.Stats(); cached != 2 {
		t.Fatalf("expected the cache capped at 2, got %d", cached)
	}
	now := byURI(c.Route([]LikeCountUpdate{
		{SubjectURI: "at://old", Increment: 1},
		{SubjectURI: "at://c", Increment: 1},
	}))
	if now["at://old"] != 1 || len(now) != 1 {
		t.Fatalf("expected only the evicted post written immediately, got %v", now)
	}
}

func TestCoalescer_NilWritesEverything(t *testing.T) {
	var c *LikeCountCoalescer
	updates := []LikeCountUpdate{{SubjectURI: "at://a", Increment: 1}, {SubjectURI: "at://a", Increment: 1}}

	if now := c.Route(updates); len(now) != 2 {
		t.Fatalf("expected a nil coalescer to pass updates through, got %v", now)
	}
	if c.Drain() != nil {
		t.Fatal("expected a nil coalescer to hold nothing")
	}
	c.Observe([]LikeCountResult{{AtURI: "at://a", LikeCount: 1}})
}

func TestCoalescer_ConcurrentRoutingLosesNoLikes(t *testing.T) {
	c := NewLikeCountCoalescer(10, 1000)
	posts := []string{"at://hot1", "at://hot2", "at://cold"}
	c.Observe([]LikeCountResult{{AtURI: "at://hot1", LikeCount: 500}, {AtURI: "at://hot2", LikeCount: 20}})

	var mu sync.Mutex
	written := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				for _, u := range c.Route([]LikeCountUpdate{{SubjectURI: posts[i%len(posts)], Increment: 1}}) {
					mu.Lock()
					written[u.SubjectURI] += u.Increment
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	for _, u := range c.Drain() {
		written[u.SubjectURI] += u.Increment
	}

	total := 0
	for _, v := range written {
		total += v
	}
	if total != 8*250 {
		t.Fatalf("expected all %d likes accounted for, got %d (%v)", 8*250, total, written)
	}
}
