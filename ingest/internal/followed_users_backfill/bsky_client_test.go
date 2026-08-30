package followed_users_backfill

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func followsPage(dids []string, cursor string) map[string]interface{} {
	follows := make([]map[string]string, len(dids))
	for i, d := range dids {
		follows[i] = map[string]string{"did": d}
	}
	page := map[string]interface{}{"follows": follows}
	if cursor != "" {
		page["cursor"] = cursor
	}
	return page
}

func TestFetchFollows_SinglePageComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a", "did:plc:b"}, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if !result.Complete || len(result.DIDs) != 2 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestFetchFollows_PaginatesUntilCursorExhausted(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("cursor") == "" {
			_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a"}, "page2"))
			return
		}
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:b"}, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if calls != 2 || !result.Complete || len(result.DIDs) != 2 {
		t.Errorf("unexpected result after %d calls: %+v", calls, result)
	}
}

func TestFetchFollows_StopsAtLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a", "did:plc:b", "did:plc:c"}, "more"))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 3)
	if err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if !result.Complete || len(result.DIDs) != 3 {
		t.Errorf("expected exactly 3 dids and complete=true (limit reached on its own terms), got %+v", result)
	}
}

func TestFetchFollows_PartialOnPageErrorAfterFirstPage(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a"}, "page2"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	client.maxRetries = 0 // fail immediately, don't wait out the retry backoff in the test
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err != nil {
		t.Fatalf("expected partial result, not error: %v", err)
	}
	if result.Complete || len(result.DIDs) != 1 {
		t.Errorf("expected partial result with 1 did, got %+v", result)
	}
}

func TestFetchFollows_ErrorWhenNothingCollected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	client.maxRetries = 0
	_, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err == nil {
		t.Error("expected error when zero dids could be fetched")
	}
}

func TestFetchFollows_TimeoutReturnsPartial(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a"}, "page2"))
			return
		}
		time.Sleep(50 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:b"}, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result, err := client.FetchFollows(ctx, "did:plc:user", 1000)
	if err != nil {
		t.Fatalf("expected partial result on timeout, not error: %v", err)
	}
	if result.Complete || len(result.DIDs) != 1 {
		t.Errorf("expected partial result with 1 did, got %+v", result)
	}
}
