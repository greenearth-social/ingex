package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/greenearth/ingest/internal/common"
)

// ingestionMetricHook triggers cancellation at known processing steps instead
// of relying on sleeps to race ingestion against shutdown.
type ingestionMetricHook func(string)

func (hook ingestionMetricHook) Record(name string, _ float64) { hook(name) }

func writeTestCursor(t *testing.T, path string, cursor int64) {
	t.Helper()
	data, err := json.Marshal(common.CursorState{LastTimeUs: cursor})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func readTestCursor(t *testing.T, path string) int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state common.CursorState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state.LastTimeUs
}

// ingestionTestConfig runs real ingestion code against local WebSocket and
// Elasticsearch servers. The saved cursor starts at 100 and the next event is
// at 300; expectedCursor is the checkpoint ingestion must read before connecting.
func ingestionTestConfig(t *testing.T, expectedCursor int64) (*common.Config, *common.IngestLogger, *common.HealthServer, *atomic.Int64) {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "jetstream_state.json")
	writeTestCursor(t, statePath, 100)
	var likesWritten atomic.Int64
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/_bulk" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read bulk request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if strings.Contains(string(body), `"index":{"_id":"at://did:plc:author/app.bsky.feed.like/test"`) {
				// The final checkpoint must wait for this partial batch's write.
				data, err := os.ReadFile(statePath)
				var state common.CursorState
				if err != nil || json.Unmarshal(data, &state) != nil || state.LastTimeUs != expectedCursor {
					t.Errorf("cursor advanced before the final batch finished: %s (read error: %v)", data, err)
				}
				likesWritten.Add(1)
			}
			_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(es.Close)
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cursor := r.URL.Query().Get("cursor"); cursor != fmt.Sprint(expectedCursor) {
			t.Errorf("WebSocket started with cursor %s, want %d", cursor, expectedCursor)
		}
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade WebSocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		message := `{"did":"did:plc:author","kind":"commit","time_us":300,"commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"test","record":{"createdAt":"2026-11-05T00:00:00Z","subject":{"uri":"at://did:plc:subject/app.bsky.feed.post/test"}}}}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			t.Errorf("send Jetstream message: %v", err)
			return
		}
		// Stay connected until the ingester cancels intake and closes the socket.
		_, _, _ = conn.ReadMessage()
	}))
	t.Cleanup(ws.Close)
	logger := common.NewLogger(true)
	logger.SetOutput(io.Discard)
	health, err := common.NewHealthServer(0, 0, logger)
	if err != nil {
		t.Fatal(err)
	}
	return &common.Config{
		JetstreamURL:               "ws" + strings.TrimPrefix(ws.URL, "http"),
		JetstreamStateFile:         statePath,
		ElasticsearchURL:           es.URL,
		Environment:                "prod",
		IndexPeriod:                common.IndexPeriod10Min,
		LikeRateLimitPerHour:       2000,
		LikeRateLimitWindowMinutes: 5,
		LikeBlockDurationMinutes:   60,
	}, logger, health, &likesWritten
}

func TestRunIngestionFinalCheckpoint(t *testing.T) {
	for _, loseOwnership := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_ownership=%t", loseOwnership), func(t *testing.T) {
			config, logger, health, likesWritten := ingestionTestConfig(t, 100)
			intakeCtx, stopIntake := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopIntake()
			workCtx, stopWork := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopWork()
			logger.SetMetricCollector(ingestionMetricHook(func(name string) {
				if name == "jetstream.inbound_count" {
					// Stop after one event, leaving a batch smaller than the normal flush size.
					stopIntake()
				}
				if loseOwnership && name == "like_counts.written_count" {
					// The like write succeeded, but ownership was lost before checkpointing.
					stopWork()
				}
			}))
			if err := runIngestion(intakeCtx, workCtx, config, logger, health, false, false, false, 0); err != nil {
				t.Fatal(err)
			}
			if got := likesWritten.Load(); got != 1 {
				t.Fatalf("final partial batch writes = %d, want 1", got)
			}
			wantCursor := int64(300)
			if loseOwnership {
				wantCursor = 100
			}
			if got := readTestCursor(t, config.JetstreamStateFile); got != wantCursor {
				t.Fatalf("final cursor = %d, want %d", got, wantCursor)
			}
		})
	}
}

func TestRunLocalIngestionDrainsOnShutdown(t *testing.T) {
	config, logger, health, likesWritten := ingestionTestConfig(t, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger.SetMetricCollector(ingestionMetricHook(func(name string) {
		if name == "jetstream.inbound_count" {
			// Cancel the process context with a partial batch still waiting to write.
			cancel()
		}
	}))
	err := withIngestionLease(ctx, config.JetstreamStateFile, logger, health, func(intakeCtx, workCtx context.Context) error {
		return runIngestion(intakeCtx, workCtx, config, logger, health, false, false, false, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("process context error = %v, want cancellation after the event", ctx.Err())
	}
	if got := likesWritten.Load(); got != 1 {
		t.Fatalf("local final partial batch writes = %d, want 1", got)
	}
	if got := readTestCursor(t, config.JetstreamStateFile); got != 300 {
		t.Fatalf("local final cursor = %d, want 300", got)
	}
	if _, err := os.Stat(config.JetstreamStateFile + ".lease"); !os.IsNotExist(err) {
		t.Fatalf("local ingestion created a lease file or could not check its absence: %v", err)
	}
}

// cursorHandoffLease lets the old owner's final checkpoint happen exactly when
// acquisition succeeds, exposing any cursor read performed before ownership.
type cursorHandoffLease struct {
	acquire func()
	expires time.Time
}

func (l *cursorHandoffLease) TryAcquire(context.Context, time.Duration) (bool, error) {
	l.acquire()
	return true, nil
}
func (l *cursorHandoffLease) Renew(context.Context, time.Duration) error { return nil }
func (l *cursorHandoffLease) Release(context.Context) error              { return nil }
func (l *cursorHandoffLease) ExpiresAt() time.Time                       { return l.expires }

func TestRunIngestionLoadsCursorAfterAcquiringLease(t *testing.T) {
	config, logger, health, likesWritten := ingestionTestConfig(t, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger.SetMetricCollector(ingestionMetricHook(func(name string) {
		if name == "jetstream.inbound_count" {
			cancel()
		}
	}))
	lease := &cursorHandoffLease{
		acquire: func() {
			// The old owner makes a final checkpoint immediately before handoff.
			writeTestCursor(t, config.JetstreamStateFile, 200)
		},
		expires: time.Now().Add(time.Minute),
	}
	timing := leaseTiming{
		ttl: time.Minute, renew: 20 * time.Second, poll: time.Millisecond,
		request: time.Second, margin: time.Second, drain: time.Second,
	}
	err := runWithLease(ctx, lease, timing, logger, func() {}, func(intakeCtx, workCtx context.Context) error {
		return runIngestion(intakeCtx, workCtx, config, logger, health, false, false, false, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := likesWritten.Load(); got != 1 {
		t.Fatalf("successor partial batch writes = %d, want 1", got)
	}
	if got := readTestCursor(t, config.JetstreamStateFile); got != 300 {
		t.Fatalf("successor final cursor = %d, want 300", got)
	}
}
