package perspective

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

func testClient(host string, maxRetries int) *Client {
	return NewClient(ClientConfig{
		Host:           host,
		APIKey:         "test-key",
		Timeout:        2 * time.Second,
		MaxRetries:     maxRetries,
		RetryBaseDelay: time.Millisecond,
	}, common.NewLogger(false))
}

// fullScoreResponse answers with every requested attribute at value.
func fullScoreResponse(value float64) map[string]interface{} {
	scores := make(map[string]interface{}, len(RequestedAttributes))
	for _, name := range RequestedAttributes {
		scores[name] = map[string]interface{}{
			"summaryScore": map[string]interface{}{"value": value, "type": "PROBABILITY"},
		}
	}
	return map[string]interface{}{"attributeScores": scores}
}

func TestScoreSuccess(t *testing.T) {
	var gotPath, gotKey, gotContentType string
	var gotBody struct {
		Comment struct {
			Text string `json:"text"`
		} `json:"comment"`
		RequestedAttributes map[string]struct{} `json:"requestedAttributes"`
		DoNotStore          bool                `json:"doNotStore"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.URL.Query().Get("key")
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.25))
	}))
	defer server.Close()

	scores, err := testClient(server.URL, 0).Score(context.Background(), "hello world")
	if err != nil {
		t.Fatalf("Score() error = %v", err)
	}

	if gotPath != analyzePath {
		t.Errorf("path = %q, want %q", gotPath, analyzePath)
	}
	if gotKey != "test-key" {
		t.Errorf("key query param = %q, want the API key", gotKey)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q", gotContentType)
	}
	if gotBody.Comment.Text != "hello world" {
		t.Errorf("comment text = %q", gotBody.Comment.Text)
	}
	if len(gotBody.RequestedAttributes) != len(RequestedAttributes) {
		t.Errorf("requested %d attributes, want %d", len(gotBody.RequestedAttributes), len(RequestedAttributes))
	}
	// Bluesky posts are public but there is no reason to hand Google a
	// retained copy of the firehose.
	if !gotBody.DoNotStore {
		t.Error("doNotStore = false, want true")
	}

	if len(scores) != len(RequestedAttributes) {
		t.Fatalf("got %d scores, want %d", len(scores), len(RequestedAttributes))
	}
	// Keys come back in storage form, not the API's SCREAMING_CASE.
	if got, ok := scores["moral_outrage_experimental"]; !ok || got != 0.25 {
		t.Errorf("scores[moral_outrage_experimental] = %v, %v", got, ok)
	}
	if _, ok := scores["MORAL_OUTRAGE_EXPERIMENTAL"]; ok {
		t.Error("scores are keyed by API name, want storage keys")
	}
}

// A partial response would be combined with the missing attributes treated as
// zero, which for a negatively weighted attribute reads as "not toxic" — a
// silently wrong score is worse than no score.
func TestScoreRejectsPartialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		full := fullScoreResponse(0.5)
		scores, ok := full["attributeScores"].(map[string]interface{})
		if !ok {
			t.Fatal("fullScoreResponse did not return an attributeScores map")
		}
		delete(scores, "TOXICITY")
		_ = json.NewEncoder(w).Encode(full)
	}))
	defer server.Close()

	_, err := testClient(server.URL, 0).Score(context.Background(), "text")
	if err == nil {
		t.Fatal("Score() error = nil, want an error for a missing attribute")
	}
	if !strings.Contains(err.Error(), "TOXICITY") {
		t.Errorf("error should name the missing attribute, got %v", err)
	}
}

func TestScoreLanguageNotSupported(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":400,"details":[{"errorType":"LANGUAGE_NOT_SUPPORTED_BY_ATTRIBUTE","languageNotSupportedByAttributeError":{"detectedLanguages":["ja"]}}]}}`))
	}))
	defer server.Close()

	_, err := testClient(server.URL, 3).Score(context.Background(), "日本語")
	if !errors.Is(err, ErrLanguageNotSupported) {
		t.Fatalf("Score() error = %v, want ErrLanguageNotSupported", err)
	}
	// Permanent for this text: retrying only burns quota to be refused again.
	if got := attempts.Load(); got != 1 {
		t.Errorf("made %d attempts, want 1", got)
	}
}

// A 400 that is not the language rejection is a real client error and must not
// be mistaken for one.
func TestScoreOtherBadRequestIsNotLanguageError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":400,"message":"Comment text too long"}}`))
	}))
	defer server.Close()

	_, err := testClient(server.URL, 0).Score(context.Background(), "text")
	if err == nil {
		t.Fatal("Score() error = nil, want an error")
	}
	if errors.Is(err, ErrLanguageNotSupported) {
		t.Error("a generic 400 was reported as ErrLanguageNotSupported")
	}
}

func TestScoreRetriesServerErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(fullScoreResponse(0.1))
	}))
	defer server.Close()

	scores, err := testClient(server.URL, 3).Score(context.Background(), "text")
	if err != nil {
		t.Fatalf("Score() error = %v, want success after retries", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("made %d attempts, want 3", got)
	}
	if len(scores) != len(RequestedAttributes) {
		t.Errorf("got %d scores after retry", len(scores))
	}
}

func TestScoreGivesUpAfterMaxRetries(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	if _, err := testClient(server.URL, 2).Score(context.Background(), "text"); err == nil {
		t.Fatal("Score() error = nil, want failure")
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("made %d attempts, want 3 (1 + 2 retries)", got)
	}
}

func TestNewClientDefaultsToGoogleHost(t *testing.T) {
	client := NewClient(ClientConfig{APIKey: "k"}, common.NewLogger(false))
	if client.config.Host != DefaultHost {
		t.Errorf("host = %q, want %q", client.config.Host, DefaultHost)
	}
}
