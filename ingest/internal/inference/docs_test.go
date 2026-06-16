package inference

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/greenearth/ingest/internal/common"
)

func makeDoc(atURI, authorDID, threadParentPost string, contentEmbedding []float32) common.ElasticsearchDoc {
	doc := common.ElasticsearchDoc{
		AtURI:            atURI,
		AuthorDID:        authorDID,
		ThreadParentPost: threadParentPost,
	}
	if contentEmbedding != nil {
		doc.Embeddings = map[string]common.Float32Array{
			contentEmbeddingKey: contentEmbedding,
		}
	}
	return doc
}

func TestAttachPostTowerEmbeddings(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 32, 4, common.NewLogger(false))

	docs := []common.ElasticsearchDoc{
		makeDoc("at://a/post/1", "did:plc:a", "", []float32{1.0, 0.5}),
		makeDoc("at://a/post/2", "did:plc:a", "at://parent/post/0", []float32{2.0, 0.5}), // reply: skipped
		makeDoc("at://a/post/3", "did:plc:b", "", nil),                                   // no content embedding: skipped
		makeDoc("at://a/post/4", "did:plc:b", "", []float32{4.0, 0.5}),
	}

	embedded, skipped, failed := AttachPostTowerEmbeddings(context.Background(), embedder, docs)

	if embedded != 2 || skipped != 2 || failed != 0 {
		t.Errorf("counts = (%d, %d, %d), want (2, 2, 0)", embedded, skipped, failed)
	}

	if got := docs[0].Embeddings[postEmbeddingKey]; len(got) != 2 || got[0] != 1.0 {
		t.Errorf("doc 0 %s = %v, want [1.0, 1.0]", postEmbeddingKey, got)
	}
	if _, ok := docs[1].Embeddings[postEmbeddingKey]; ok {
		t.Errorf("doc 1 is a reply, must not have %s", postEmbeddingKey)
	}
	if _, ok := docs[2].Embeddings[postEmbeddingKey]; ok {
		t.Errorf("doc 2 has no content embedding, must not have %s", postEmbeddingKey)
	}
	if got := docs[3].Embeddings[postEmbeddingKey]; len(got) != 2 || got[0] != 4.0 {
		t.Errorf("doc 3 %s = %v, want [4.0, 4.0]", postEmbeddingKey, got)
	}

	// Content embeddings must be preserved
	if got := docs[0].Embeddings[contentEmbeddingKey]; len(got) != 2 || got[0] != 1.0 {
		t.Errorf("doc 0 content embedding was clobbered: %v", got)
	}

	// Model UUID must be set on eligible docs and empty on skipped ones
	if docs[0].PostEmbeddingModelUUID != "test-uuid-echo" {
		t.Errorf("doc 0 PostEmbeddingModelUUID = %q, want %q", docs[0].PostEmbeddingModelUUID, "test-uuid-echo")
	}
	if docs[1].PostEmbeddingModelUUID != "" {
		t.Errorf("doc 1 (reply) PostEmbeddingModelUUID = %q, want empty", docs[1].PostEmbeddingModelUUID)
	}
	if docs[2].PostEmbeddingModelUUID != "" {
		t.Errorf("doc 2 (no content embedding) PostEmbeddingModelUUID = %q, want empty", docs[2].PostEmbeddingModelUUID)
	}
	if docs[3].PostEmbeddingModelUUID != "test-uuid-echo" {
		t.Errorf("doc 3 PostEmbeddingModelUUID = %q, want %q", docs[3].PostEmbeddingModelUUID, "test-uuid-echo")
	}
}

func TestAttachPostTowerEmbeddingsNilEmbedder(t *testing.T) {
	docs := []common.ElasticsearchDoc{
		makeDoc("at://a/post/1", "did:plc:a", "", []float32{1.0, 0.5}),
	}

	embedded, skipped, failed := AttachPostTowerEmbeddings(context.Background(), nil, docs)

	if embedded != 0 || skipped != 0 || failed != 0 {
		t.Errorf("counts = (%d, %d, %d), want (0, 0, 0) for nil embedder", embedded, skipped, failed)
	}
	if _, ok := docs[0].Embeddings[postEmbeddingKey]; ok {
		t.Errorf("nil embedder must not attach embeddings")
	}
}

func TestAttachPostTowerEmbeddingsEmptyDocs(t *testing.T) {
	var requests, maxInFlight atomic.Int32
	server := echoTowerServer(t, &requests, &maxInFlight)
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 32, 4, common.NewLogger(false))
	embedded, skipped, failed := AttachPostTowerEmbeddings(context.Background(), embedder, nil)

	if embedded != 0 || skipped != 0 || failed != 0 {
		t.Errorf("counts = (%d, %d, %d), want (0, 0, 0)", embedded, skipped, failed)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("request count = %d, want 0", got)
	}
}

func TestAttachPostTowerEmbeddingsFailOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadRequest)
	}))
	defer server.Close()

	embedder := NewBatchEmbedder(testClient(server.URL, 0), 32, 4, common.NewLogger(false))
	docs := []common.ElasticsearchDoc{
		makeDoc("at://a/post/1", "did:plc:a", "", []float32{1.0, 0.5}),
		makeDoc("at://a/post/2", "did:plc:a", "at://parent/post/0", []float32{2.0, 0.5}),
	}

	embedded, skipped, failed := AttachPostTowerEmbeddings(context.Background(), embedder, docs)

	if embedded != 0 || skipped != 1 || failed != 1 {
		t.Errorf("counts = (%d, %d, %d), want (0, 1, 1)", embedded, skipped, failed)
	}
	if _, ok := docs[0].Embeddings[postEmbeddingKey]; ok {
		t.Errorf("failed doc must not have %s", postEmbeddingKey)
	}
	// Doc must still be marshalable/indexable without the post embedding
	if _, err := json.Marshal(docs[0]); err != nil {
		t.Errorf("doc 0 not marshalable after failure: %v", err)
	}
	if docs[0].PostEmbeddingModelUUID != "" {
		t.Errorf("failed doc PostEmbeddingModelUUID = %q, want empty", docs[0].PostEmbeddingModelUUID)
	}
}
