package common

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func topicRejectionsForTest(atURI string, values ...interface{}) topicScoreRejections {
	var rejected topicScoreRejections
	for i, value := range values {
		rejected.add(atURI, fmt.Sprintf("Topic %d", i), value)
	}
	return rejected
}

func TestTopicScoreRejectionReasons(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		reason int
	}{
		{"negative", -0.1, topicScoreOutOfRange},
		{"above one", 1.1, topicScoreOutOfRange},
		{"null", nil, topicScoreNull},
		{"string", "0.5", topicScoreWrongType},
		{"boolean", true, topicScoreWrongType},
		{"object", map[string]interface{}{"score": 0.5}, topicScoreWrongType},
		{"array", []interface{}{0.5}, topicScoreWrongType},
		// These exercise the existing numeric guard, not JSON NaN parsing.
		{"nonfinite", math.NaN(), topicScoreNonFinite},
		{"positive infinity", math.Inf(1), topicScoreNonFinite},
		{"negative infinity", math.Inf(-1), topicScoreNonFinite},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rejected := topicRejectionsForTest("at://test", tt.value)
			if rejected.total() != 1 || rejected.counts[tt.reason] != 1 {
				t.Fatalf("counts = %v, want one %s rejection", rejected.counts, topicScoreReasonNames[tt.reason])
			}
			if len(rejected.examples) != 1 || !strings.Contains(rejected.examples[0], "reason="+topicScoreReasonNames[tt.reason]) {
				t.Errorf("examples = %v, want reason %s", rejected.examples, topicScoreReasonNames[tt.reason])
			}
		})
	}
}

func TestTopicScoreLogIntervalAndAggregation(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&output)
	metrics := newMockMetricCollector()
	logger.SetMetricCollector(metrics)
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	logger.topicScoreLog.now = func() time.Time { return now }

	logger.reportInvalidTopicScores(topicRejectionsForTest("at://first", nil))
	if strings.Count(output.String(), "[ERROR]") != 1 {
		t.Fatalf("first rejection must log immediately: %s", output.String())
	}
	for i := 0; i < 5; i++ {
		logger.reportInvalidTopicScores(topicRejectionsForTest(fmt.Sprintf("at://suppressed-%d", i), -0.1, 1.1, "bad"))
	}
	now = now.Add(topicScoreLogInterval - time.Nanosecond)
	logger.reportInvalidTopicScores(topicRejectionsForTest("at://before-boundary", nil))
	if strings.Count(output.String(), "[ERROR]") != 1 {
		t.Fatalf("logged inside the reporting interval: %s", output.String())
	}
	if logger.topicScoreLog.pending.total() != 16 || logger.topicScoreLog.affectedPosts != 6 {
		t.Fatalf("suppressed counts not accumulated: %+v", logger.topicScoreLog.pending)
	}
	if len(logger.topicScoreLog.pending.examples) != topicScoreMaxExamples {
		t.Fatalf("pending examples = %d, want bounded sample of %d", len(logger.topicScoreLog.pending.examples), topicScoreMaxExamples)
	}
	// Counters must already include suppressed errors, before a summary is due.
	assertTopicScoreMetricTotal(t, metrics, "invalid_count", 17)
	assertTopicScoreMetricTotal(t, metrics, "affected_post_count", 7)

	now = now.Add(time.Nanosecond)
	logger.reportInvalidTopicScores(topicRejectionsForTest("at://boundary", -2.0, 2.0))
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want two: %s", len(lines), output.String())
	}
	for _, field := range []string{"invalid_scores=18", "affected_posts=7", "out_of_range=12", "null=1", "wrong_type=5", "non_finite=0", "expected_range=[0,1]"} {
		if !strings.Contains(lines[1], field) {
			t.Errorf("summary missing %q: %s", field, lines[1])
		}
	}
	if strings.Count(lines[1], "{at_uri=") != topicScoreMaxExamples {
		t.Errorf("summary must have exactly %d examples: %s", topicScoreMaxExamples, lines[1])
	}
	if logger.topicScoreLog.pending.total() != 0 || len(logger.topicScoreLog.pending.examples) != 0 || logger.topicScoreLog.affectedPosts != 0 {
		t.Fatal("pending counts and examples must reset after a summary")
	}

	now = now.Add(topicScoreLogInterval)
	logger.reportInvalidTopicScores(topicRejectionsForTest("at://next-window", nil))
	lines = strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[2], "invalid_scores=1 affected_posts=1 out_of_range=0 null=1 wrong_type=0") {
		t.Fatalf("next summary double-counted an earlier window: %s", output.String())
	}
	assertTopicScoreMetricTotal(t, metrics, "invalid_count", 20)
	assertTopicScoreMetricTotal(t, metrics, "affected_post_count", 9)
	assertTopicScoreMetricTotal(t, metrics, "out_of_range_count", 12)
	assertTopicScoreMetricTotal(t, metrics, "null_count", 3)
	assertTopicScoreMetricTotal(t, metrics, "wrong_type_count", 5)
}

func TestTopicScoreLogConcurrentPosts(t *testing.T) {
	var output bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&output)
	metrics := newMockMetricCollector()
	logger.SetMetricCollector(metrics)
	logger.topicScoreLog.now = func() time.Time { return time.Unix(1, 0) }

	const posts = 1000
	var wg sync.WaitGroup
	for i := 0; i < posts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.reportInvalidTopicScores(topicRejectionsForTest(fmt.Sprintf("at://post-%d", i), nil, 1.1, "bad"))
		}(i)
	}
	wg.Wait()
	if strings.Count(output.String(), "[ERROR]") != 1 {
		t.Fatalf("concurrent posts bypassed shared limit: %s", output.String())
	}
	assertTopicScoreMetricTotal(t, metrics, "invalid_count", posts*3)
	assertTopicScoreMetricTotal(t, metrics, "affected_post_count", posts)
	for _, reason := range []string{"null_count", "out_of_range_count", "wrong_type_count"} {
		assertTopicScoreMetricTotal(t, metrics, reason, posts)
	}
	if logger.topicScoreLog.pending.total() != (posts-1)*3 || logger.topicScoreLog.affectedPosts != posts-1 {
		t.Fatal("lost pending counts under concurrent access")
	}
	if len(logger.topicScoreLog.pending.examples) != topicScoreMaxExamples {
		t.Fatal("concurrent reporting did not retain a bounded sample")
	}
}

func TestTopicScoreLogDisabledAndEmpty(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			var output bytes.Buffer
			logger := NewLogger(enabled)
			logger.SetOutput(&output)
			metrics := newMockMetricCollector()
			logger.SetMetricCollector(metrics)
			logger.reportInvalidTopicScores(topicScoreRejections{})
			if !enabled {
				logger.reportInvalidTopicScores(topicRejectionsForTest("at://test", nil, 2.0))
			}
			if output.Len() != 0 || len(metrics.records) != 0 || logger.topicScoreLog.pending.total() != 0 {
				t.Fatal("disabled/empty reporting must not log, record metrics, or accumulate state")
			}
		})
	}
}

func TestTopicScoreLogExamplesAreBounded(t *testing.T) {
	var rejected topicScoreRejections
	for i := 0; i < 100; i++ {
		rejected.add(strings.Repeat("u", 10_000), strings.Repeat("l", 10_000), strings.Repeat("v", 10_000))
	}
	if rejected.total() != 100 || len(rejected.examples) != topicScoreMaxExamples {
		t.Fatalf("counts/examples = %d/%d", rejected.total(), len(rejected.examples))
	}
	for _, example := range rejected.examples {
		if len(example) > 600 || !strings.Contains(example, "...") {
			t.Fatalf("oversized or unmarked truncated example (%d bytes)", len(example))
		}
	}
	for _, value := range []interface{}{map[string]interface{}{"payload": "do-not-log-this"}, []interface{}{"do-not-log-this"}} {
		example := topicRejectionsForTest("at://test", value).examples[0]
		if strings.Contains(example, "do-not-log-this") || !strings.Contains(example, "value=<omitted>") {
			t.Fatalf("unexpected payload leaked into example: %s", example)
		}
	}
	// A string containing line breaks must not create additional log lines.
	example := topicRejectionsForTest("at://test\nuri", "invalid\nvalue").examples[0]
	if strings.Contains(example, "\n") {
		t.Fatalf("unescaped newline in example: %q", example)
	}
}

func assertTopicScoreMetricTotal(t *testing.T, metrics *mockMetricCollector, suffix string, want float64) {
	t.Helper()
	var got float64
	for _, value := range metrics.getRecords("megastream.topic_scores." + suffix) {
		got += value
	}
	if got != want {
		t.Errorf("%s total = %v, want %v", suffix, got, want)
	}
}
