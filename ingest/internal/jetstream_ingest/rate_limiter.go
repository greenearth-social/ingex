package jetstream_ingest

import (
	"context"
	"sync"
	"time"
)

// BlockEntry records when an account was blocked and for how long.
type BlockEntry struct {
	BlockedAt time.Time
	Duration  time.Duration
}

// RateLimiter tracks per-account like rates and blocks accounts that exceed the threshold.
type RateLimiter struct {
	mu             sync.Mutex
	windowCounts   map[string]int
	blocked        map[string]BlockEntry
	windowDuration time.Duration
	blockDuration  time.Duration
	threshold      int
}

// NewRateLimiter creates a RateLimiter with the given window duration, block duration, and threshold.
func NewRateLimiter(windowDur, blockDur time.Duration, threshold int) *RateLimiter {
	return &RateLimiter{
		windowCounts:   make(map[string]int),
		blocked:        make(map[string]BlockEntry),
		windowDuration: windowDur,
		blockDuration:  blockDur,
		threshold:      threshold,
	}
}

// RecordLike records a like from the given DID and returns whether the account is blocked
// and whether it was newly blocked by this call.
func (rl *RateLimiter) RecordLike(did string) (blocked bool, newlyBlocked bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Check if already blocked (and not expired)
	if entry, ok := rl.blocked[did]; ok {
		if time.Since(entry.BlockedAt) < entry.Duration {
			return true, false
		}
		// Block expired — remove it and reset the window count so the
		// account gets a clean slate for the current window.
		delete(rl.blocked, did)
		delete(rl.windowCounts, did)
	}

	rl.windowCounts[did]++
	if rl.windowCounts[did] >= rl.threshold {
		rl.blocked[did] = BlockEntry{
			BlockedAt: time.Now(),
			Duration:  rl.blockDuration,
		}
		return true, true
	}

	return false, false
}

// GetBlockedAccounts returns a snapshot of currently active (non-expired) block entries.
func (rl *RateLimiter) GetBlockedAccounts() map[string]BlockEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	snapshot := make(map[string]BlockEntry, len(rl.blocked))
	for did, entry := range rl.blocked {
		if now.Sub(entry.BlockedAt) < entry.Duration {
			snapshot[did] = entry
		}
	}
	return snapshot
}

// resetWindow clears all per-window like counts. Called periodically by Start.
func (rl *RateLimiter) resetWindow() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.windowCounts = make(map[string]int)
}

// Start launches the background goroutine that resets window counts every windowDuration.
func (rl *RateLimiter) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(rl.windowDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.resetWindow()
			}
		}
	}()
}
