package common

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

// scrollESHandler answers the three requests an author scroll makes: the
// initial search, one empty scroll page, and the clear-scroll DELETE.
func scrollESHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	switch {
	case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/_search/scroll"):
		_, _ = w.Write([]byte(`{"succeeded":true,"num_freed":1}`))
	default:
		_, _ = w.Write([]byte(`{"_scroll_id":"abc","hits":{"hits":[]}}`))
	}
}

// newConnCountingESClient returns a client whose server counts the TCP
// connections it accepts. A response body left open pins its connection, so
// the next request has to dial a new one.
func newConnCountingESClient(t *testing.T) (*elasticsearch.Client, *atomic.Int64) {
	t.Helper()
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(scrollESHandler))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	client, err := elasticsearch.New(elasticsearch.WithAddresses(srv.URL))
	if err != nil {
		t.Fatalf("failed to create mock ES client: %v", err)
	}
	return client, &conns
}

// Account deletion runs these scrolls three times per deleted account, so a
// leaked clear-scroll connection grows sockets and memory without bound.
func TestAuthorScrolls_ReuseConnections(t *testing.T) {
	const calls = 20
	logger := NewLogger(false)

	t.Run("posts", func(t *testing.T) {
		client, conns := newConnCountingESClient(t)
		for i := 0; i < calls; i++ {
			if _, err := QueryPostsByAuthorDID(t.Context(), client, "posts", "did:plc:test", logger); err != nil {
				t.Fatalf("QueryPostsByAuthorDID: %v", err)
			}
		}
		if got := conns.Load(); got > 2 {
			t.Errorf("expected connections to be reused, server accepted %d over %d calls", got, calls)
		}
	})

	t.Run("likes", func(t *testing.T) {
		client, conns := newConnCountingESClient(t)
		for i := 0; i < calls; i++ {
			if _, err := QueryLikesByAuthorDID(t.Context(), client, "likes", "did:plc:test", logger); err != nil {
				t.Fatalf("QueryLikesByAuthorDID: %v", err)
			}
		}
		if got := conns.Load(); got > 2 {
			t.Errorf("expected connections to be reused, server accepted %d over %d calls", got, calls)
		}
	})
}
