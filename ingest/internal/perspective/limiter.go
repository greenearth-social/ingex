package perspective

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/greenearth/ingest/internal/common"
)

// QuotaPolicy decides what happens when the rate limiter has no slot free.
type QuotaPolicy string

const (
	// QuotaWait blocks until a slot frees up. Because megastream_ingest
	// serializes batch flushes (see drainPendingFlush), blocking here
	// backpressures ingestion, which is what we want by default: falling
	// behind is recoverable, and a gap in the scored corpus is the thing this
	// story exists to avoid.
	QuotaWait QuotaPolicy = "wait"
	// QuotaSkip indexes the post unscored rather than waiting. The escape
	// hatch for when serving traffic spikes and ingest has to yield the
	// shared quota; posts skipped this way are recovered by
	// cmd/backfill_perspective.
	QuotaSkip QuotaPolicy = "skip"
)

// ParseQuotaPolicy validates a configured policy string.
func ParseQuotaPolicy(s string) (QuotaPolicy, error) {
	switch QuotaPolicy(s) {
	case QuotaWait:
		return QuotaWait, nil
	case QuotaSkip:
		return QuotaSkip, nil
	default:
		return "", fmt.Errorf("invalid quota policy %q (want %q or %q)", s, QuotaWait, QuotaSkip)
	}
}

// ErrQuotaExhausted is returned when a slot was refused under QuotaSkip.
var ErrQuotaExhausted = errors.New("perspective quota exhausted")

// limiter caps requests to our share of the Perspective quota.
//
// The quota is 36 000 requests per minute, shared between this service and the
// api's serving path, so ingest takes a configured slice of it — 8 460 RPM by
// default, against serving's 26 700. A token bucket rather than the
// calendar-minute counter the api uses, because the refill rate bounds
// sustained draw over every window rather than resetting on a wall-clock edge.
//
// Burst capacity is one megastream batch, not one second of budget. Perspective
// meters per minute, so pacing a batch out over a second-by-second allowance
// buys nothing it asks for and costs the whole batch its latency: a 236-post
// batch against a 15/s bucket took ~14s to admit, against 93ms of actual API
// time per call. The serving path already sends Perspective bursts of this
// shape on every ranking request, so the burst itself is not novel traffic.
//
// The arithmetic that matters: a bucket with burst B and rate R admits at most
// B + R×T over a window T, so the true per-minute ceiling is R×60 + B, not
// R×60. The slice numbers above are chosen against that, not against R×60 —
// 141×60 + 512 = 8 972, inside 9 000 with the 300 RPM buffer untouched. Raising
// perspectiveBurst or GE_PERSPECTIVE_QPS means redoing that sum.
//
// Note this is a per-process limit. That is correct here only because
// megastream_ingest runs a single instance (it owns one cursor); anything that
// scales out would need a shared counter.
//
// It is also per *environment*, which the 9 000 figure does not by itself
// account for: stage and prod deploy into the same GCP project and Perspective
// quota is per project, so stage ingest spends from the same pool. Its ceiling
// is scaled down at deploy time instead (scripts/deploy.sh), by the same factor
// ShouldSampleDID drops posts at, since stage ingests a tenth of the stream and
// so can never want more than a tenth of the rate.
type limiter struct {
	rl     *rate.Limiter
	policy QuotaPolicy
	logger *common.IngestLogger
}

// perspectiveBurst is the bucket's burst capacity, sized to one megastream
// flush (batchSize in cmd/megastream_ingest) so a whole batch is admitted
// without pacing. The two are coupled only by this comment; a change to the
// flush size wants a change here.
const perspectiveBurst = 512

func newLimiter(qps, burst int, policy QuotaPolicy, logger *common.IngestLogger) *limiter {
	if qps <= 0 {
		qps = 1
	}
	// A burst below the rate would make the bucket stricter than its own
	// refill, which is never what a caller means.
	if burst < qps {
		burst = qps
	}
	return &limiter{
		rl:     rate.NewLimiter(rate.Limit(qps), burst),
		policy: policy,
		logger: logger,
	}
}

// acquire takes a slot, or returns ErrQuotaExhausted under QuotaSkip.
//
// Waits are measured and reported whether or not they succeed: rising
// perspective.rate_limit.wait_ms is the signal that the budget is becoming the
// binding constraint, and it shows up well before anything is dropped.
func (l *limiter) acquire(ctx context.Context) error {
	if l.policy == QuotaSkip {
		if l.rl.Allow() {
			return nil
		}
		l.logger.Metric("perspective.rate_limit.skipped.count", 1)
		return ErrQuotaExhausted
	}

	// Reserve first so the wait can be measured rather than inferred. A
	// reservation that cannot be honoured at all (ctx already past its
	// deadline) is cancelled so it does not consume budget.
	reservation := l.rl.Reserve()
	if !reservation.OK() {
		l.logger.Metric("perspective.rate_limit.skipped.count", 1)
		return ErrQuotaExhausted
	}

	delay := reservation.Delay()
	if delay <= 0 {
		return nil
	}

	l.logger.Metric("perspective.rate_limit.throttled.count", 1)
	l.logger.Metric("perspective.rate_limit.wait_ms", float64(delay.Milliseconds()))

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		reservation.Cancel()
		l.logger.Metric("perspective.rate_limit.skipped.count", 1)
		return ctx.Err()
	}
}
