package common

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

// mockESHandler returns an HTTP handler that responds to ES index creation requests.
type mockESHandler struct {
	statusCode int
	body       string
}

func (m *mockESHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.WriteHeader(m.statusCode)
	_, _ = w.Write([]byte(m.body))
}

func newMockESClient(t *testing.T, handler http.Handler) (*elasticsearch.Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{srv.URL},
	})
	if err != nil {
		srv.Close()
		t.Fatalf("failed to create mock ES client: %v", err)
	}
	return client, srv
}

func TestEnsureIndex_NonResourceAlreadyExistsError_IncludesBody(t *testing.T) {
	errBody := `{"error":{"type":"illegal_argument_exception","reason":"too many shards"},"status":400}`
	handler := &mockESHandler{statusCode: 400, body: errBody}
	client, srv := newMockESClient(t, handler)
	defer srv.Close()

	logger := NewLogger(false)
	err := EnsureIndex(t.Context(), client, "likes-2026-04-27-00", "likes", logger)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "illegal_argument_exception") {
		t.Errorf("error should contain the ES error type from the response body; got: %v", err)
	}
	if !strings.Contains(err.Error(), "too many shards") {
		t.Errorf("error should contain the ES reason from the response body; got: %v", err)
	}
}

func TestEnsureIndex_ResourceAlreadyExists_IsNotAnError(t *testing.T) {
	// Sequence of responses:
	// 1. PUT /likes-2026-04-27-00 → 400 resource_already_exists_exception
	// 2. GET /_alias/likes → 200 with index already as write target
	// Subsequent calls return 404 for anything else.
	responses := []struct {
		code int
		body string
	}{
		// Create index → already exists
		{400, `{"error":{"type":"resource_already_exists_exception","reason":"index already exists"},"status":400}`},
		// GetAlias → index is already the write target
		{200, `{"likes-2026-04-27-00":{"aliases":{"likes":{"is_write_index":true}}}}`},
	}
	idx := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if idx < len(responses) {
			w.WriteHeader(responses[idx].code)
			_, _ = w.Write([]byte(responses[idx].body))
			idx++
		} else {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("failed to create ES client: %v", err)
	}

	logger := NewLogger(false)
	err = EnsureIndex(t.Context(), client, "likes-2026-04-27-00", "likes", logger)
	if err != nil {
		t.Errorf("expected no error when index already exists as write target; got: %v", err)
	}
}

func TestEnsureIndex_EmptyErrorBody_ReturnsStatusCode(t *testing.T) {
	handler := &mockESHandler{statusCode: 400, body: ""}
	client, srv := newMockESClient(t, handler)
	defer srv.Close()

	logger := NewLogger(false)
	err := EnsureIndex(t.Context(), client, "likes-2026-04-27-00", "likes", logger)

	if err == nil {
		t.Fatal("expected an error for empty 400 body, got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should include the HTTP status code; got: %v", err)
	}
}
