package common

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	topicScoreLogInterval = time.Minute
	topicScoreMaxExamples = 3
)

const (
	topicScoreOutOfRange = iota
	topicScoreNull
	topicScoreWrongType
	topicScoreNonFinite
	topicScoreReasonCount
)

var topicScoreReasonNames = [topicScoreReasonCount]string{
	"out_of_range", "null", "wrong_type", "non_finite",
}

// topicScoreRejections holds counts and a bounded sample for one post. Missing
// analyses are not rejections: only present, invalid score values reach add.
type topicScoreRejections struct {
	counts   [topicScoreReasonCount]uint64
	examples []string
}

func (r *topicScoreRejections) total() uint64 {
	var total uint64
	for _, count := range r.counts {
		total += count
	}
	return total
}

// add is called only after a score has failed validation.
func (r *topicScoreRejections) add(atURI, label string, value interface{}) {
	reason := topicScoreOutOfRange
	if value == nil {
		reason = topicScoreNull
	} else if score, ok := value.(float64); !ok {
		reason = topicScoreWrongType
	} else if math.IsNaN(score) || math.IsInf(score, 0) {
		reason = topicScoreNonFinite
	}
	r.counts[reason]++
	if len(r.examples) < topicScoreMaxExamples {
		r.examples = append(r.examples, fmt.Sprintf("{at_uri=%q label=%q value=%s type=%T reason=%s}",
			boundedTopicScoreString(atURI, 256), boundedTopicScoreString(label, 96),
			topicScoreLogValue(value), value, topicScoreReasonNames[reason]))
	}
}

func boundedTopicScoreString(value string, maxBytes int) string {
	if len(value) > maxBytes {
		return value[:maxBytes] + "..."
	}
	return value
}

// Never serialize an unexpected object/array into a log. Strings and identifiers
// are bounded too, so an upstream schema change cannot produce huge log lines.
func topicScoreLogValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case string:
		return fmt.Sprintf("%q", boundedTopicScoreString(v, 96))
	default:
		return "<omitted>"
	}
}

// The ingester shares its logger across posts/workers, so this limit applies
// across messages, not once per post or label. No background goroutine is needed:
// the first rejection logs immediately, then subsequent rejections trigger at
// most one summary per minute. Pending counts are included in the next summary;
// metrics record every rejection immediately, even if no later rejection arrives.
type topicScoreRejectionLog struct {
	mu            sync.Mutex
	lastReport    time.Time
	pending       topicScoreRejections
	affectedPosts uint64
	now           func() time.Time // Optional clock override for deterministic tests.
}

func (l *IngestLogger) reportInvalidTopicScores(rejected topicScoreRejections) {
	if !l.enabled || rejected.total() == 0 {
		return
	}

	// Fixed metric names keep cardinality bounded; labels/URIs appear only in
	// sampled examples. Counters remain complete when error logs are suppressed.
	l.Metric("megastream.topic_scores.invalid_count", float64(rejected.total()))
	l.Metric("megastream.topic_scores.affected_post_count", 1)
	for reason, count := range rejected.counts {
		if count > 0 {
			l.Metric("megastream.topic_scores."+topicScoreReasonNames[reason]+"_count", float64(count))
		}
	}

	r := &l.topicScoreLog
	r.mu.Lock()
	defer r.mu.Unlock()
	for reason, count := range rejected.counts {
		r.pending.counts[reason] += count
	}
	r.affectedPosts++
	remaining := topicScoreMaxExamples - len(r.pending.examples)
	if remaining > 0 {
		r.pending.examples = append(r.pending.examples, rejected.examples[:min(remaining, len(rejected.examples))]...)
	}

	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if !r.lastReport.IsZero() && now.Sub(r.lastReport) < topicScoreLogInterval {
		return
	}
	l.Error("Dropped invalid topic scores: invalid_scores=%d affected_posts=%d out_of_range=%d null=%d wrong_type=%d non_finite=%d expected_range=[0,1] examples=[%s] (counts since previous report; logs limited to once per %s)",
		r.pending.total(), r.affectedPosts,
		r.pending.counts[topicScoreOutOfRange], r.pending.counts[topicScoreNull],
		r.pending.counts[topicScoreWrongType], r.pending.counts[topicScoreNonFinite],
		strings.Join(r.pending.examples, "; "), topicScoreLogInterval)
	r.lastReport = now
	r.pending = topicScoreRejections{}
	r.affectedPosts = 0
}
