package perspective

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// recordingCollector captures metric emissions so the tests can assert on the
// signals an operator actually watches, not just on control flow.
type recordingCollector struct {
	mu     sync.Mutex
	values map[string][]float64
}

func newRecordingCollector() *recordingCollector {
	return &recordingCollector{values: map[string][]float64{}}
}

func (c *recordingCollector) Record(name string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[name] = append(c.values[name], value)
}

func (c *recordingCollector) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.values[name])
}

func loggerWithCollector(c *recordingCollector) *common.IngestLogger {
	// Metric() is a no-op on a disabled logger, so this must be enabled;
	// SetOutput sends the log lines nowhere so tests stay quiet.
	logger := common.NewLogger(true)
	logger.SetOutput(io.Discard)
	logger.SetMetricCollector(c)
	return logger
}

func TestLimiterAllowsWithinBudget(t *testing.T) {
	collector := newRecordingCollector()
	l := newLimiter(100, 100, QuotaWait, loggerWithCollector(collector))

	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("acquire() error = %v", err)
	}
	if collector.count("perspective.rate_limit.throttled.count") != 0 {
		t.Error("a request inside the budget was reported as throttled")
	}
}

// Wait mode blocks rather than dropping: falling behind is recoverable, a gap
// in the scored corpus is what the backfill exists to repair.
func TestLimiterWaitModeBlocksAndReportsWait(t *testing.T) {
	collector := newRecordingCollector()
	// 1 QPS, burst 1: the second acquire must wait about a second.
	l := newLimiter(1, 1, QuotaWait, loggerWithCollector(collector))

	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire() error = %v", err)
	}

	start := time.Now()
	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("second acquire() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("second acquire returned after %v, expected it to block", elapsed)
	}

	if collector.count("perspective.rate_limit.throttled.count") != 1 {
		t.Error("a blocked request was not counted as throttled")
	}
	if collector.count("perspective.rate_limit.wait_ms") != 1 {
		t.Error("a blocked request did not report its wait time")
	}
	// Waiting is not dropping.
	if collector.count("perspective.rate_limit.skipped.count") != 0 {
		t.Error("wait mode reported a skip")
	}
}

// Skip mode is the escape hatch when serving needs the shared quota: index the
// post unscored rather than slowing ingestion down.
func TestLimiterSkipModeDropsAndCounts(t *testing.T) {
	collector := newRecordingCollector()
	l := newLimiter(1, 1, QuotaSkip, loggerWithCollector(collector))

	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire() error = %v", err)
	}

	start := time.Now()
	err := l.acquire(context.Background())
	if !errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("second acquire() error = %v, want ErrQuotaExhausted", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("skip mode blocked for %v; it must return immediately", elapsed)
	}

	// This is the metric that says a backfill is owed.
	if collector.count("perspective.rate_limit.skipped.count") != 1 {
		t.Error("a dropped request was not counted as skipped")
	}
}

func TestLimiterWaitModeRespectsContextCancellation(t *testing.T) {
	collector := newRecordingCollector()
	l := newLimiter(1, 1, QuotaWait, loggerWithCollector(collector))

	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := l.acquire(ctx); err == nil {
		t.Fatal("acquire() error = nil, want the context error")
	}
	if collector.count("perspective.rate_limit.skipped.count") != 1 {
		t.Error("a request abandoned mid-wait was not counted as skipped")
	}
}

func TestParseQuotaPolicy(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    QuotaPolicy
		wantErr bool
	}{
		{"wait", QuotaWait, false},
		{"skip", QuotaSkip, false},
		{"", "", true},
		{"drop", "", true},
		{"WAIT", "", true},
	} {
		got, err := ParseQuotaPolicy(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseQuotaPolicy(%q) error = nil, want an error", tc.in)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseQuotaPolicy(%q) = %v, %v; want %v, nil", tc.in, got, err, tc.want)
		}
	}
}

// A megastream batch must be admitted without pacing: Perspective meters per
// minute, so spending a batch's latency budget on a per-second allowance buys
// nothing. Burst is what makes that true, and it is easy to regress by
// "tidying" newLimiter back to a one-second bucket.
func TestLimiterAdmitsAWholeBatchWithoutWaiting(t *testing.T) {
	collector := newRecordingCollector()
	// The shape production runs: a rate far below the batch size, and a burst
	// sized to the batch.
	l := newLimiter(15, perspectiveBurst, QuotaWait, loggerWithCollector(collector))

	const batch = 236 // posts in a stage batch, measured
	start := time.Now()
	for i := 0; i < batch; i++ {
		if err := l.acquire(context.Background()); err != nil {
			t.Fatalf("acquire(%d) error = %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("admitting %d requests took %v; a batch-sized burst should not pace them", batch, elapsed)
	}
	if n := collector.count("perspective.rate_limit.throttled.count"); n != 0 {
		t.Errorf("throttled %d requests inside the burst", n)
	}
}

// The refill rate, not the burst, is what bounds sustained draw: once the
// bucket is spent the configured rate takes over. Without this the burst could
// be raised arbitrarily and nothing would fail.
func TestLimiterStillPacesOnceBurstIsSpent(t *testing.T) {
	collector := newRecordingCollector()
	l := newLimiter(1, 4, QuotaWait, loggerWithCollector(collector))

	for i := 0; i < 4; i++ {
		if err := l.acquire(context.Background()); err != nil {
			t.Fatalf("acquire(%d) error = %v", i, err)
		}
	}
	if n := collector.count("perspective.rate_limit.throttled.count"); n != 0 {
		t.Fatalf("throttled %d requests inside the burst of 4", n)
	}

	start := time.Now()
	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("acquire past the burst error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("the request past the burst returned after %v; it should be paced at 1/s", elapsed)
	}
	if collector.count("perspective.rate_limit.throttled.count") != 1 {
		t.Error("the request past the burst was not counted as throttled")
	}
}

// A caller asking for a burst below the rate gets the rate: a bucket stricter
// than its own refill is never what was meant.
func TestLimiterRaisesBurstToAtLeastTheRate(t *testing.T) {
	collector := newRecordingCollector()
	l := newLimiter(10, 2, QuotaWait, loggerWithCollector(collector))

	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := l.acquire(context.Background()); err != nil {
			t.Fatalf("acquire(%d) error = %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("10 acquires at 10 qps took %v; burst should have been raised to the rate", elapsed)
	}
}
