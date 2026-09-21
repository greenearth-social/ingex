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
// api's serving path, so ingest takes a configured slice of it — 9 000 RPM by
// default, against serving's 26 700, which leaves a 300 RPM buffer for the
// inexactness of two independent limiters. The limiter is
// deliberately a smooth token bucket rather than the calendar-minute counter
// the api uses: a minute bucket permits spending the entire allowance in the
// first second of each minute, which is exactly the burst shape that would
// collide with a serving spike. rate.Limiter spreads the same budget evenly.
//
// Note this is a per-process limit. That is correct here only because
// megastream_ingest runs a single instance (it owns one cursor); anything that
// scales out would need a shared counter.
type limiter struct {
	rl     *rate.Limiter
	policy QuotaPolicy
	logger *common.IngestLogger
}

func newLimiter(qps int, policy QuotaPolicy, logger *common.IngestLogger) *limiter {
	if qps <= 0 {
		qps = 1
	}
	return &limiter{
		// Burst equal to one second of budget: enough that a batch's fan-out
		// starts immediately, small enough that we never present the API with
		// a spike larger than our sustained rate.
		rl:     rate.NewLimiter(rate.Limit(qps), qps),
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
