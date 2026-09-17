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
	l := newLimiter(100, QuotaWait, loggerWithCollector(collector))

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
	l := newLimiter(1, QuotaWait, loggerWithCollector(collector))

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
	l := newLimiter(1, QuotaSkip, loggerWithCollector(collector))

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
	l := newLimiter(1, QuotaWait, loggerWithCollector(collector))

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
