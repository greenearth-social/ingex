package elasticsearch_expiry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/greenearth/ingest/internal/common"
)

// esResponse is one canned reply from the mock Elasticsearch server.
type esResponse struct {
	code int
	body string
}

// newTestService stands up a mock Elasticsearch that replays responses in
// order (repeating the last one once exhausted) and returns a Service wired to
// it, along with a counter of requests received. Retry delays are shortened so
// the tests exercise the backoff loop without sleeping for a minute.
func newTestService(t *testing.T, config Config, responses ...esResponse) (*Service, *atomic.Int32) {
	t.Helper()

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(requests.Add(1)) - 1
		if i >= len(responses) {
			i = len(responses) - 1
		}
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(responses[i].code)
		_, _ = w.Write([]byte(responses[i].body))
	}))
	t.Cleanup(srv.Close)

	client, err := elasticsearch.New(
		elasticsearch.WithAddresses(srv.URL),
		// The transport's own retries would obscure the counts this test
		// asserts on; the retry under test is the one in ExpireCollection.
		elasticsearch.WithTransportOptions(elastictransport.WithDisableRetry()),
	)
	if err != nil {
		t.Fatalf("failed to create mock ES client: %v", err)
	}

	service := NewService(client, config, common.NewLogger(false))
	service.initialRetryDelay = time.Millisecond
	service.maxRetryDelay = 2 * time.Millisecond
	return service, &requests
}

func deleteConfig() Config {
	return Config{CutoffDate: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}
}

// The 503 that has been failing hashtag expiry in stage: the cluster is
// briefly RED while shards recover from a node replacement.
const missingShards503 = `{"error":{"root_cause":[],"type":"search_phase_execution_exception","reason":"start","phase":"can_match","caused_by":{"type":"search_phase_execution_exception","reason":"Search rejected due to missing shards [[hashtags_v1][0], [hashtags_v1][1]]."}},"status":503}`

func TestExpireCollection_RetriesTransient503ThenSucceeds(t *testing.T) {
	service, requests := newTestService(t, deleteConfig(),
		esResponse{503, missingShards503},
		esResponse{503, missingShards503},
		esResponse{200, `{"deleted":42,"version_conflicts":0,"timed_out":false,"took":10,"failures":[]}`},
	)

	deleted, err := service.ExpireCollection(t.Context(), Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err != nil {
		t.Fatalf("expected the retries to ride out the transient 503, got: %v", err)
	}
	if deleted != 42 {
		t.Errorf("deleted = %d, want 42", deleted)
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("made %d requests, want 3 (two failures then a success)", got)
	}
}

func TestExpireCollection_RetriesTransient401(t *testing.T) {
	// The API key is authenticated at client construction and injected once at
	// task start, so a mid-run 401 means ES could not read its security index.
	unauthorized := `{"error":{"root_cause":[{"type":"security_exception","reason":"unable to authenticate with provided credentials and anonymous access is not allowed for this request"}],"status":401}`
	service, requests := newTestService(t, deleteConfig(),
		esResponse{401, unauthorized},
		esResponse{200, `{"deleted":7,"version_conflicts":0,"timed_out":false,"took":3,"failures":[]}`},
	)

	deleted, err := service.ExpireCollection(t.Context(), Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err != nil {
		t.Fatalf("expected 401 to be retried, got: %v", err)
	}
	if deleted != 7 {
		t.Errorf("deleted = %d, want 7", deleted)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("made %d requests, want 2", got)
	}
}

func TestExpireCollection_GivesUpAfterMaxAttempts(t *testing.T) {
	service, requests := newTestService(t, deleteConfig(), esResponse{503, missingShards503})

	_, err := service.ExpireCollection(t.Context(), Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err == nil {
		t.Fatal("expected an error once the retries are exhausted, got nil")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should carry the last Elasticsearch failure; got: %v", err)
	}
	if got := int(requests.Load()); got != maxAttempts {
		t.Errorf("made %d requests, want %d", got, maxAttempts)
	}
}

func TestExpireCollection_DoesNotRetryClientError(t *testing.T) {
	badRequest := `{"error":{"type":"illegal_argument_exception","reason":"unknown field [hour]"},"status":400}`
	service, requests := newTestService(t, deleteConfig(), esResponse{400, badRequest})

	_, err := service.ExpireCollection(t.Context(), Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "illegal_argument_exception") {
		t.Errorf("error should carry the Elasticsearch reason; got: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("made %d requests, want 1 — a malformed request will not fix itself", got)
	}
}

func TestExpireCollection_StopsRetryingWhenContextCancelled(t *testing.T) {
	service, requests := newTestService(t, deleteConfig(), esResponse{503, missingShards503})
	// Long enough that the test would hang if cancellation were not honoured.
	service.initialRetryDelay = time.Hour
	service.maxRetryDelay = time.Hour

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		for requests.Load() < 1 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	_, err := service.ExpireCollection(ctx, Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err == nil {
		t.Fatal("expected an error after cancellation, got nil")
	}
}

func TestExpireCollection_DryRunRetriesTransientCount(t *testing.T) {
	config := deleteConfig()
	config.DryRun = true
	service, requests := newTestService(t, config,
		esResponse{503, missingShards503},
		esResponse{200, `{"count":11}`},
	)

	count, err := service.ExpireCollection(t.Context(), Collection{IndexAlias: "hashtags", DateField: "hour"})

	if err != nil {
		t.Fatalf("expected the dry-run count to be retried, got: %v", err)
	}
	if count != 11 {
		t.Errorf("count = %d, want 11", count)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("made %d requests, want 2", got)
	}
}
