package jetstream_ingest

import (
	"testing"
	"time"
)

func TestRateLimiterNotBlocked(t *testing.T) {
	rl := NewRateLimiter(5*time.Minute, time.Hour, 10)

	for i := 0; i < 9; i++ {
		blocked, newlyBlocked := rl.RecordLike("did:plc:test")
		if blocked {
			t.Errorf("iteration %d: expected not blocked, got blocked", i)
		}
		if newlyBlocked {
			t.Errorf("iteration %d: expected newlyBlocked=false", i)
		}
	}
}

func TestRateLimiterBlocksAtThreshold(t *testing.T) {
	rl := NewRateLimiter(5*time.Minute, time.Hour, 10)

	// Record likes up to threshold - 1 (should not be blocked)
	for i := 0; i < 9; i++ {
		blocked, _ := rl.RecordLike("did:plc:test")
		if blocked {
			t.Fatalf("iteration %d: should not be blocked yet", i)
		}
	}

	// 10th like hits threshold — should be newly blocked
	blocked, newlyBlocked := rl.RecordLike("did:plc:test")
	if !blocked {
		t.Error("expected blocked=true at threshold")
	}
	if !newlyBlocked {
		t.Error("expected newlyBlocked=true at threshold")
	}
}

func TestRateLimiterDropsBlockedLikes(t *testing.T) {
	rl := NewRateLimiter(5*time.Minute, time.Hour, 3)

	// Hit threshold
	for i := 0; i < 3; i++ {
		rl.RecordLike("did:plc:spammer")
	}

	// Subsequent likes from the same account
	for i := 0; i < 5; i++ {
		blocked, newlyBlocked := rl.RecordLike("did:plc:spammer")
		if !blocked {
			t.Errorf("iteration %d: expected blocked=true for already-blocked account", i)
		}
		if newlyBlocked {
			t.Errorf("iteration %d: expected newlyBlocked=false for already-blocked account", i)
		}
	}
}

func TestRateLimiterBlockExpiry(t *testing.T) {
	blockDur := 50 * time.Millisecond
	rl := NewRateLimiter(5*time.Minute, blockDur, 2)

	// Block the account
	rl.RecordLike("did:plc:temp")
	rl.RecordLike("did:plc:temp")

	// Verify blocked
	blocked, _ := rl.RecordLike("did:plc:temp")
	if !blocked {
		t.Fatal("expected account to be blocked")
	}

	// Wait for block to expire
	time.Sleep(blockDur + 10*time.Millisecond)

	// Should be unblocked now
	blocked, newlyBlocked := rl.RecordLike("did:plc:temp")
	if blocked {
		t.Error("expected account to be unblocked after block duration")
	}
	if newlyBlocked {
		t.Error("expected newlyBlocked=false after expiry")
	}
}

func TestRateLimiterWindowReset(t *testing.T) {
	windowDur := 50 * time.Millisecond
	rl := NewRateLimiter(windowDur, time.Hour, 5)

	// Record 4 likes (below threshold of 5)
	for i := 0; i < 4; i++ {
		rl.RecordLike("did:plc:test")
	}

	// Manually trigger a window reset (simulates ticker firing)
	rl.resetWindow()

	// After reset, count should be 0; threshold should not be hit until 5 more
	for i := 0; i < 4; i++ {
		blocked, _ := rl.RecordLike("did:plc:test")
		if blocked {
			t.Errorf("iteration %d: should not be blocked after window reset", i)
		}
	}
}

func TestRateLimiterGetBlockedAccounts(t *testing.T) {
	rl := NewRateLimiter(5*time.Minute, time.Hour, 2)

	// Block two accounts
	rl.RecordLike("did:plc:spam1")
	rl.RecordLike("did:plc:spam1")

	rl.RecordLike("did:plc:spam2")
	rl.RecordLike("did:plc:spam2")

	// Normal account stays unblocked
	rl.RecordLike("did:plc:normal")

	snapshot := rl.GetBlockedAccounts()

	if len(snapshot) != 2 {
		t.Errorf("expected 2 blocked accounts, got %d", len(snapshot))
	}
	if _, ok := snapshot["did:plc:spam1"]; !ok {
		t.Error("expected did:plc:spam1 in blocked accounts")
	}
	if _, ok := snapshot["did:plc:spam2"]; !ok {
		t.Error("expected did:plc:spam2 in blocked accounts")
	}
	if _, ok := snapshot["did:plc:normal"]; ok {
		t.Error("did:plc:normal should not be in blocked accounts")
	}
}

func TestRateLimiterGetBlockedAccountsExcludesExpired(t *testing.T) {
	blockDur := 50 * time.Millisecond
	rl := NewRateLimiter(5*time.Minute, blockDur, 2)

	// Block account
	rl.RecordLike("did:plc:expiring")
	rl.RecordLike("did:plc:expiring")

	// Wait for expiry
	time.Sleep(blockDur + 10*time.Millisecond)

	snapshot := rl.GetBlockedAccounts()
	if _, ok := snapshot["did:plc:expiring"]; ok {
		t.Error("expired block should not appear in GetBlockedAccounts")
	}
}
