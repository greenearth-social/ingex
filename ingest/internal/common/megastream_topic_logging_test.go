package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const topicLoggingPrivateContent = "private post content must not appear in topic rejection logs"

func topicLoggingRawPost(content string) string {
	return fmt.Sprintf(`{"message":{"commit":{"operation":"create","record":{"text":%q}}}}`, content)
}

func topicLoggingInferences(t *testing.T, topics map[string]interface{}) string {
	t.Helper()
	inferences := map[string]interface{}{
		"text": map[string]interface{}{
			"message.commit.record.text": map[string]interface{}{"topic": topics},
		},
	}
	encoded, err := json.Marshal(inferences)
	if err != nil {
		t.Fatalf("marshal topic inferences: %v", err)
	}
	return string(encoded)
}

func newTopicLoggingTestLogger() (*IngestLogger, *bytes.Buffer, *mockMetricCollector) {
	logger := NewLogger(true)
	output := new(bytes.Buffer)
	metrics := newMockMetricCollector()
	logger.SetOutput(output)
	logger.SetMetricCollector(metrics)
	return logger, output, metrics
}

func assertTopicLoggingMetricTotals(t *testing.T, metrics *mockMetricCollector, want map[string]float64) {
	t.Helper()
	for _, suffix := range []string{
		"invalid_count", "affected_post_count", "out_of_range_count",
		"null_count", "wrong_type_count", "non_finite_count",
	} {
		var total float64
		for _, value := range metrics.getRecords("megastream.topic_scores." + suffix) {
			total += value
		}
		if total != want[suffix] {
			t.Errorf("metric %s total = %v, want %v", suffix, total, want[suffix])
		}
	}
}

func TestMegaStreamMessage_TopicReportingQuietForAbsentOrValidScores(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		inferences string
		want       map[string]float32
	}{
		{
			name:       "empty body without any analyses",
			inferences: `{}`,
		},
		{
			name:       "nonempty body without any analyses",
			content:    topicLoggingPrivateContent,
			inferences: `{}`,
		},
		{
			name:       "empty topic object",
			content:    topicLoggingPrivateContent,
			inferences: `{"text":{"message.commit.record.text":{"topic":{}}}}`,
		},
		{
			name:       "body analysis without topics",
			content:    topicLoggingPrivateContent,
			inferences: `{"text":{"message.commit.record.text":{"sentiment":{"positive":0.8}}}}`,
		},
		{
			name: "empty body with link-only analyses",
			inferences: `{"text":{
				"message.commit.record.embed.external.title":{"topic":{"News & Social Concern":0.6}},
				"message.commit.record.embed.external.description":{"topic":{"News & Social Concern":0.7,"_truncated":true}}
			}}`,
		},
		{
			name:       "invalid link topics are outside the body-topic contract",
			inferences: `{"text":{"message.commit.record.embed.external.title":{"topic":{"Sports":2,"Music":null}}}}`,
		},
		{
			name:    "valid scores including both range boundaries",
			content: topicLoggingPrivateContent,
			inferences: topicLoggingInferences(t, map[string]interface{}{
				"News & Social Concern": 0.5,
				"Sports":                0,
				"Music":                 1,
			}),
			want: map[string]float32{"News & Social Concern": 0.5, "Sports": 0, "Music": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, output, metrics := newTopicLoggingTestLogger()
			msg := NewMegaStreamMessage("at://quiet", "did:plc:test", topicLoggingRawPost(tt.content), tt.inferences, logger)
			if got := msg.GetTopicScores(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTopicScores() = %v, want %v", got, tt.want)
			}
			if output.Len() != 0 {
				t.Errorf("expected no topic rejection logging, got %q", output.String())
			}
			if len(metrics.records) != 0 {
				t.Errorf("expected no rejection metrics, got %v", metrics.records)
			}
		})
	}
}

func TestMegaStreamMessage_TopicReportingDoesNotExemptTruncatedLabel(t *testing.T) {
	logger, output, metrics := newTopicLoggingTestLogger()
	msg := NewMegaStreamMessage("at://metadata", "did:plc:test", topicLoggingRawPost(topicLoggingPrivateContent),
		topicLoggingInferences(t, map[string]interface{}{"_truncated": true, "Sports": 0.75}), logger)
	want := map[string]float32{"Sports": 0.75}
	if got := msg.GetTopicScores(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetTopicScores() = %v, want %v", got, want)
	}
	assertTopicLoggingMetricTotals(t, metrics, map[string]float64{
		"invalid_count": 1, "affected_post_count": 1, "wrong_type_count": 1,
	})
	if !strings.Contains(output.String(), "_truncated") || !strings.Contains(output.String(), "wrong_type=1") {
		t.Errorf("expected ordinary invalid-score reporting for _truncated, got %q", output.String())
	}
}

func TestMegaStreamMessage_TopicReportingClassifiesInvalidScores(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		reason string
	}{
		{name: "negative", value: -0.01, reason: "out_of_range"},
		{name: "greater than one", value: 1.01, reason: "out_of_range"},
		{name: "explicit null", value: nil, reason: "null"},
		{name: "numeric string", value: "0.5", reason: "wrong_type"},
		{name: "true", value: true, reason: "wrong_type"},
		{name: "false", value: false, reason: "wrong_type"},
		{name: "object", value: map[string]interface{}{"payload": "sensitive object payload"}, reason: "wrong_type"},
		{name: "array", value: []interface{}{"sensitive array payload"}, reason: "wrong_type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, output, metrics := newTopicLoggingTestLogger()
			msg := NewMegaStreamMessage("at://invalid", "did:plc:test", topicLoggingRawPost(topicLoggingPrivateContent),
				topicLoggingInferences(t, map[string]interface{}{"News & Social Concern": tt.value, "Sports": 0.5}), logger)
			want := map[string]float32{"Sports": 0.5}
			if got := msg.GetTopicScores(); !reflect.DeepEqual(got, want) {
				t.Errorf("GetTopicScores() = %v, want %v", got, want)
			}
			assertTopicLoggingMetricTotals(t, metrics, map[string]float64{
				"invalid_count": 1, "affected_post_count": 1, tt.reason + "_count": 1,
			})
			logged := output.String()
			for _, required := range []string{"[ERROR]", "Dropped invalid topic scores:", "invalid_scores=1", "affected_posts=1", tt.reason + "=1", "expected_range=[0,1]", "at://invalid", "News & Social Concern"} {
				if !strings.Contains(logged, required) {
					t.Errorf("log missing %q: %s", required, logged)
				}
			}
			if strings.Count(logged, "\n") != 1 {
				t.Errorf("expected one error line, got %q", logged)
			}
			for _, private := range []string{topicLoggingPrivateContent, "sensitive object payload", "sensitive array payload"} {
				if strings.Contains(logged, private) {
					t.Errorf("log unexpectedly contains private post or nonscalar content %q", private)
				}
			}
		})
	}
}

func TestMegaStreamMessage_TopicReportingAggregatesWholePost(t *testing.T) {
	logger, output, metrics := newTopicLoggingTestLogger()
	msg := NewMegaStreamMessage("at://mixed", "did:plc:test", topicLoggingRawPost(topicLoggingPrivateContent),
		topicLoggingInferences(t, map[string]interface{}{
			"Below": -0.1, "Above": 1.1, "Null": nil,
			"String": "0.5", "True": true, "False": false,
			"Object": map[string]interface{}{}, "Array": []interface{}{},
			"Sports": 0.75, "_truncated": true,
		}), logger)
	if got, want := msg.GetTopicScores(), (map[string]float32{"Sports": 0.75}); !reflect.DeepEqual(got, want) {
		t.Errorf("GetTopicScores() = %v, want %v", got, want)
	}
	assertTopicLoggingMetricTotals(t, metrics, map[string]float64{
		"invalid_count": 9, "affected_post_count": 1,
		"out_of_range_count": 2, "null_count": 1, "wrong_type_count": 6,
	})
	logged := output.String()
	for _, required := range []string{"invalid_scores=9", "affected_posts=1", "out_of_range=2", "null=1", "wrong_type=6", "non_finite=0"} {
		if !strings.Contains(logged, required) {
			t.Errorf("whole-post summary missing %q: %s", required, logged)
		}
	}
	if strings.Count(logged, "[ERROR]") != 1 || strings.Count(logged, "\n") != 1 {
		t.Errorf("multiple invalid labels must generate a single error line, got %q", logged)
	}
}

func TestMegaStreamMessage_TopicReportingCountsSuppressedPosts(t *testing.T) {
	logger, output, metrics := newTopicLoggingTestLogger()
	const numPosts = 100
	inferences := topicLoggingInferences(t, map[string]interface{}{
		"News & Social Concern": 2, "Music": nil, "Sports": 0.5,
	})
	for i := 0; i < numPosts; i++ {
		msg := NewMegaStreamMessage(fmt.Sprintf("at://burst/%d", i), "did:plc:test", topicLoggingRawPost(topicLoggingPrivateContent), inferences, logger)
		if got, want := msg.GetTopicScores(), (map[string]float32{"Sports": 0.5}); !reflect.DeepEqual(got, want) {
			t.Fatalf("post %d GetTopicScores() = %v, want %v", i, got, want)
		}
	}
	assertTopicLoggingMetricTotals(t, metrics, map[string]float64{
		"invalid_count": 2 * numPosts, "affected_post_count": numPosts,
		"out_of_range_count": numPosts, "null_count": numPosts,
	})
	if got := strings.Count(output.String(), "[ERROR]"); got != 1 {
		t.Errorf("shared logger generated %d errors for a burst of %d posts, want 1: %s", got, numPosts, output.String())
	}
}

func TestMegaStreamMessage_TopicReportingDoesNotAbortOtherInferences(t *testing.T) {
	logger, output, metrics := newTopicLoggingTestLogger()
	msg := NewMegaStreamMessage("at://all-invalid", "did:plc:test", topicLoggingRawPost(topicLoggingPrivateContent), `{
		"text":{"message.commit.record.text":{"topic":{"News & Social Concern":null}}},
		"video":{"audio_transcription":{"text":"retained transcript","language":"en"}}
	}`, logger)
	if got := msg.GetTopicScores(); got != nil {
		t.Errorf("GetTopicScores() = %v, want nil when every score is invalid", got)
	}
	if got := msg.GetVideoTranscript(); got != "retained transcript" {
		t.Errorf("GetVideoTranscript() = %q; rejected topics must not abort other inference parsing", got)
	}
	if got := msg.GetVideoTranscriptLanguage(); got != "en" {
		t.Errorf("GetVideoTranscriptLanguage() = %q, want en", got)
	}
	assertTopicLoggingMetricTotals(t, metrics, map[string]float64{
		"invalid_count": 1, "affected_post_count": 1, "null_count": 1,
	})
	if got := strings.Count(output.String(), "[ERROR]"); got != 1 {
		t.Errorf("all-invalid topics generated %d errors, want 1", got)
	}
}
