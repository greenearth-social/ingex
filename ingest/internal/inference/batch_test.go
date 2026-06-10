package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
)

// echoTowerServer returns a mock inference server whose output for each input
// embedding is [firstValue, firstValue] so results are attributable to inputs.
// It also tracks total requests and the maximum number of concurrent requests.
func echoTowerServer(t *testing.T, requests, maxInFlight *atomic.Int32) *httptest.Server {
	t.Helper()
	var inFlight atomic.Int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			observed := maxInFlight.Load()
			if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)

		var req predictRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		outputs := make([][]float32, len(req.PostEmbeddings))
		for i, emb := range req.PostEmbeddings {
			outputs[i] = []float32{emb[0], emb[0]}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    outputs,
			"model_type": "post-tower",
		})
	}))
}

func makeInputs(n int) []PostEmbeddingInput {
	inputs := make([]PostEmbeddingInput, n)
	for i := range inputs {
		inputs[i] = PostEmbeddingInput{
			AtURI:     fmt.Sprintf("at://did:plc:test/app.bsky.feed.post/%d", i),
			Embedding: []float32{float32(i), 0.5},
			AuthorDID: fmt.Sprintf("did:plc:author%d", i),
		}
	}
	return inputs
}

func TestComputePostEmbeddingsPreservesOrderAcrossChunks(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 7, 4, common.NewLogger(false))
	inputs := makeInputs(100)

	results := embedder.ComputePostEmbeddings(context.Background(), inputs)

	if len(results) != 100 {
		t.Fatalf("results length = %d, want 100", len(results))
	}
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("result %d unexpected error: %v", i, res.Err)
		}
		if res.AtURI != inputs[i].AtURI {
			t.Errorf("result %d AtURI = %q, want %q", i, res.AtURI, inputs[i].AtURI)
		}
		if len(res.Embedding) != 2 || res.Embedding[0] != float32(i) {
			t.Errorf("result %d embedding = %v, want [%d, %d]", i, res.Embedding, i, i)
		}
	}
}

func TestComputePostEmbeddingsChunkingMath(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 32, 8, common.NewLogger(false))
	results := embedder.ComputePostEmbeddings(context.Background(), makeInputs(100))

	if len(results) != 100 {
		t.Fatalf("results length = %d, want 100", len(results))
	}
	if got := requests.Load(); got != 4 {
		t.Errorf("request count = %d, want 4 (100 inputs / chunk size 32)", got)
	}
}

func TestComputePostEmbeddingsConcurrencyBound(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 5, 3, common.NewLogger(false))
	embedder.ComputePostEmbeddings(context.Background(), makeInputs(100))

	if got := requests.Load(); got != 20 {
		t.Errorf("request count = %d, want 20 (100 inputs / chunk size 5)", got)
	}
	if got := maxInFlight.Load(); got > 3 {
		t.Errorf("max in-flight requests = %d, want <= 3", got)
	}
}

func TestComputePostEmbeddingsFailedChunkIsIsolated(t *testing.T) {
	// Fails any chunk containing an embedding with first value 0 (the first
	// chunk); other chunks succeed.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req predictRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		if req.PostEmbeddings[0][0] == 0 {
			http.Error(w, "boom", http.StatusBadRequest)
			return
		}
		outputs := make([][]float32, len(req.PostEmbeddings))
		for i, emb := range req.PostEmbeddings {
			outputs[i] = []float32{emb[0], emb[0]}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"outputs":    outputs,
			"model_type": "post-tower",
		})
	}))
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 10, 4, common.NewLogger(false))
	results := embedder.ComputePostEmbeddings(context.Background(), makeInputs(30))

	if len(results) != 30 {
		t.Fatalf("results length = %d, want 30", len(results))
	}
	for i, res := range results {
		if i < 10 {
			if res.Err == nil {
				t.Errorf("result %d Err = nil, want error (failed chunk)", i)
			}
			if res.Embedding != nil {
				t.Errorf("result %d embedding = %v, want nil (failed chunk)", i, res.Embedding)
			}
		} else {
			if res.Err != nil {
				t.Errorf("result %d unexpected error: %v", i, res.Err)
			}
			if len(res.Embedding) != 2 {
				t.Errorf("result %d embedding = %v, want 2 values", i, res.Embedding)
			}
		}
	}
}

func TestComputePostEmbeddingsEmptyInput(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 32, 8, common.NewLogger(false))
	results := embedder.ComputePostEmbeddings(context.Background(), nil)

	if len(results) != 0 {
		t.Errorf("results length = %d, want 0", len(results))
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("request count = %d, want 0", got)
	}
}

func TestComputePostEmbeddingsCancelledContext(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 10, 4, common.NewLogger(false))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := embedder.ComputePostEmbeddings(ctx, makeInputs(30))

	if len(results) != 30 {
		t.Fatalf("results length = %d, want 30", len(results))
	}
	for i, res := range results {
		if res.Err == nil {
			t.Errorf("result %d Err = nil, want context error", i)
		}
	}
}
