package inference

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

func testClient(baseURL string, maxRetries int) *Client {
	return NewClient(ClientConfig{
		BaseURL:        baseURL,
		APIKey:         "test-key",
		Timeout:        2 * time.Second,
		MaxRetries:     maxRetries,
		RetryBaseDelay: time.Millisecond,
	}, common.NewLogger(false))
}

type predictRequest struct {
	PostEmbeddings   [][]float32 `json:"post_embeddings"`
	TargetAuthorDIDs []string    `json:"target_author_dids"`
}

func TestPostTowerPredictSuccess(t *testing.T) {
	var gotPath, gotAPIKey, gotContentType string
	var gotBody predictRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAPIKey = r.Header.Get("X-API-Key")
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		outputs := make([][]float32, len(gotBody.PostEmbeddings))
		for i := range outputs {
			outputs[i] = []float32{float32(i), float32(i) + 0.5}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    outputs,
			"model_type": "post-tower",
		})
	}))
	defer server.Close()

	client := testClient(server.URL, 0)
	inputs := [][]float32{{0.1, 0.2}, {0.3, 0.4}}
	dids := []string{"did:plc:aaa", "did:plc:bbb"}

	outputs, err := client.PostTowerPredict(context.Background(), inputs, dids)
	if err != nil {
		t.Fatalf("PostTowerPredict() error = %v, expected nil", err)
	}

	if gotPath != "/models/post-tower/predict" {
		t.Errorf("path = %q, want /models/post-tower/predict", gotPath)
	}
	if gotAPIKey != "test-key" {
		t.Errorf("X-API-Key = %q, want test-key", gotAPIKey)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if len(gotBody.PostEmbeddings) != 2 || len(gotBody.TargetAuthorDIDs) != 2 {
		t.Fatalf("request body lengths = %d/%d, want 2/2", len(gotBody.PostEmbeddings), len(gotBody.TargetAuthorDIDs))
	}
	if gotBody.TargetAuthorDIDs[0] != "did:plc:aaa" || gotBody.TargetAuthorDIDs[1] != "did:plc:bbb" {
		t.Errorf("target_author_dids = %v", gotBody.TargetAuthorDIDs)
	}
	if len(outputs) != 2 {
		t.Fatalf("outputs length = %d, want 2", len(outputs))
	}
	if outputs[0][0] != 0 || outputs[0][1] != 0.5 || outputs[1][0] != 1 || outputs[1][1] != 1.5 {
		t.Errorf("outputs = %v, order not preserved", outputs)
	}
}

func TestPostTowerPredictBadRequestNoRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, `{"detail":"batch too large (max=1024)"}`, http.StatusBadRequest)
	}))
	defer server.Close()

	client := testClient(server.URL, 3)
	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected error")
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("request count = %d, want 1 (4xx must not retry)", got)
	}
}

func TestPostTowerPredictRetriesServerError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    [][]float32{{1.0}},
			"model_type": "post-tower",
		})
	}))
	defer server.Close()

	client := testClient(server.URL, 3)
	outputs, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a"})
	if err != nil {
		t.Fatalf("PostTowerPredict() error = %v, expected nil after retry", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("request count = %d, want 2 (500 then 200)", got)
	}
	if len(outputs) != 1 || outputs[0][0] != 1.0 {
		t.Errorf("outputs = %v, want [[1.0]]", outputs)
	}
}

func TestPostTowerPredictRetriesExhausted(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := testClient(server.URL, 2)
	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected error after exhausted retries")
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("request count = %d, want 3 (1 initial + 2 retries)", got)
	}
}

func TestPostTowerPredictRetriesTooManyRequests(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    [][]float32{{1.0}},
			"model_type": "post-tower",
		})
	}))
	defer server.Close()

	client := testClient(server.URL, 3)
	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a"})
	if err != nil {
		t.Fatalf("PostTowerPredict() error = %v, expected nil after 429 retry", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("request count = %d, want 2 (429 then 200)", got)
	}
}

func TestPostTowerPredictOutputCountMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    [][]float32{{1.0}},
			"model_type": "post-tower",
		})
	}))
	defer server.Close()

	client := testClient(server.URL, 0)
	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}, {0.2}}, []string{"did:plc:a", "did:plc:b"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected output count mismatch error")
	}
}

func TestPostTowerPredictInputLengthMismatch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	client := testClient(server.URL, 0)
	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a", "did:plc:b"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected input length mismatch error")
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("request count = %d, want 0 (validation happens before any call)", got)
	}
}

func TestPostTowerPredictEmptyInput(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	client := testClient(server.URL, 0)
	outputs, err := client.PostTowerPredict(context.Background(), [][]float32{}, []string{})
	if err != nil {
		t.Fatalf("PostTowerPredict() error = %v, expected nil for empty input", err)
	}
	if len(outputs) != 0 {
		t.Errorf("outputs length = %d, want 0", len(outputs))
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("request count = %d, want 0 (empty input makes no call)", got)
	}
}

func TestPostTowerPredictTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	client := NewClient(ClientConfig{
		BaseURL:        server.URL,
		APIKey:         "test-key",
		Timeout:        20 * time.Millisecond,
		MaxRetries:     0,
		RetryBaseDelay: time.Millisecond,
	}, common.NewLogger(false))

	_, err := client.PostTowerPredict(context.Background(), [][]float32{{0.1}}, []string{"did:plc:a"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected timeout error")
	}
}

func TestPostTowerPredictContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	client := testClient(server.URL, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.PostTowerPredict(ctx, [][]float32{{0.1}}, []string{"did:plc:a"})
	if err == nil {
		t.Fatal("PostTowerPredict() error = nil, expected context cancellation error")
	}
}
