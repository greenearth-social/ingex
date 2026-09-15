package followed_users_backfill

import (
	"context"
	"encoding/json"
	"net"
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

func TestGetPage_SetsUserAgentHeader(t *testing.T) {
	var gotUserAgent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent = r.Header.Get("User-Agent")
		_ = json.NewEncoder(w).Encode(followsPage(nil, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	if _, err := client.FetchFollows(context.Background(), "did:plc:user", 10); err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if gotUserAgent != userAgent {
		t.Errorf("User-Agent header = %q, want %q", gotUserAgent, userAgent)
	}
}

func TestFetchFollows_Honors429RetryAfterHeader(t *testing.T) {
	calls := 0
	var callTimes []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callTimes = append(callTimes, time.Now())
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0") // 0s so the test stays fast but still exercises the header parse path
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a"}, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	client.retryDelay = time.Hour // if the 429 falls back to the generic retryDelay instead of Retry-After, this test times out
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if calls != 2 || !result.Complete || len(result.DIDs) != 1 {
		t.Errorf("unexpected result after %d calls: %+v", calls, result)
	}
}

func TestFetchFollows_429FallsBackToConfiguredBackoffWithoutRetryAfter(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests) // no Retry-After header
			return
		}
		_ = json.NewEncoder(w).Encode(followsPage([]string{"did:plc:a"}, ""))
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	client.rateLimitBackoff = time.Millisecond // keep the fallback backoff fast for the test
	start := time.Now()
	result, err := client.FetchFollows(context.Background(), "did:plc:user", 1000)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("FetchFollows: %v", err)
	}
	if calls != 2 || !result.Complete || len(result.DIDs) != 1 {
		t.Errorf("unexpected result after %d calls: %+v", calls, result)
	}
	if elapsed >= time.Second {
		t.Errorf("expected the configurable rateLimitBackoff to be used (fast), took %v", elapsed)
	}
}

func TestClampRetryAfterDelay(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"under cap unchanged", 5 * time.Second, 5 * time.Second},
		{"exactly at cap unchanged", maxRetryAfterDelay, maxRetryAfterDelay},
		{"over cap clamped", 10 * time.Minute, maxRetryAfterDelay},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampRetryAfterDelay(tc.in); got != tc.want {
				t.Errorf("clampRetryAfterDelay(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestRetryAfterDelay_ClampsHeaderValue(t *testing.T) {
	client := NewBskyClient(&http.Client{})
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Retry-After", "3600") // far beyond the cap
	if got := client.retryAfterDelay(resp); got != maxRetryAfterDelay {
		t.Errorf("retryAfterDelay with Retry-After=3600 = %v, want clamped to %v", got, maxRetryAfterDelay)
	}
}

func TestFetchFollows_ContextCancelledDuring429WaitReturnsPromptly(t *testing.T) {
	reached429 := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600") // would block ~forever if not clamped/cancelled
		w.WriteHeader(http.StatusTooManyRequests)
		close(reached429)
	}))
	defer srv.Close()

	client := NewBskyClient(srv.Client())
	client.baseURL = srv.URL
	client.maxRetries = 1

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-reached429
		cancel()
	}()

	start := time.Now()
	_, err := client.FetchFollows(ctx, "did:plc:user", 1000)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when ctx is cancelled during the 429 wait")
	}
	if elapsed >= time.Second {
		t.Errorf("expected prompt return on ctx cancellation, took %v", elapsed)
	}
}

func TestFetchFollows_NoRetryOnConnectionError(t *testing.T) {
	// Create a listener that immediately closes all connections (simulating connection refused)
	ctx := context.Background()
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	connCount := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connCount++
			_ = conn.Close() // Close without sending HTTP response; ignore error in cleanup
		}
	}()

	client := NewBskyClient(&http.Client{})
	client.baseURL = "http://" + listener.Addr().String()
	client.maxRetries = 1 // Set to 1 to verify it doesn't retry on connection error

	_, err = client.FetchFollows(context.Background(), "did:plc:user", 1000)
	if err == nil {
		t.Error("expected error for connection failure")
	}

	_ = listener.Close() // Stop accepting connections; ignore error in cleanup
	<-done               // Wait for goroutine to exit

	if connCount != 1 {
		t.Errorf("expected exactly 1 connection attempt (no retry on connection error), got %d", connCount)
	}
}
