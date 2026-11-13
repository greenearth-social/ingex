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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
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

// likeCountES is a fake Elasticsearch that counts the post updates in each
// like-count bulk request, calling onPostsBulk before it answers. Reply-index
// requests (each chunk writes to both) are answered but not counted.
func likeCountES(t *testing.T, onPostsBulk func(updates int)) (*elasticsearch.Client, *atomic.Int64) {
	t.Helper()
	var written atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if n := strings.Count(string(body), `"_index":"posts"`); n > 0 {
			if onPostsBulk != nil {
				onPostsBulk(n)
			}
			written.Add(int64(n))
		}
		_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
	}))
	t.Cleanup(srv.Close)
	client, err := elasticsearch.New(elasticsearch.WithAddresses(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	return client, &written
}

// heldCoalescer returns a coalescer holding one like for each of n popular posts.
func heldCoalescer(n int) *common.LikeCountCoalescer {
	c := common.NewLikeCountCoalescer(10, n)
	updates := make([]common.LikeCountUpdate, n)
	for i := range updates {
		uri := fmt.Sprintf("at://did:plc:author/app.bsky.feed.post/%d", i)
		c.Observe([]common.LikeCountResult{{AtURI: uri, LikeCount: 50}})
		updates[i] = common.LikeCountUpdate{SubjectURI: uri, Increment: 1}
	}
	c.Route(updates)
	return c
}

func quietLogger() *common.IngestLogger {
	logger := common.NewLogger(false)
	logger.SetOutput(io.Discard)
	return logger
}

// SIGTERM can land in the middle of a periodic flush, which has already
// drained the coalescer. The chunk in flight must finish rather than be
// cancelled, and every update not yet sent must be held for the final flush —
// before, the remainder was dropped on the floor.
func TestFlushLikeCountsHoldsWhatItDidNotSend(t *testing.T) {
	stopCtx, stop := context.WithCancel(context.Background())
	defer stop()
	es, written := likeCountES(t, func(int) { stop() })
	coalescer := heldCoalescer(1000)

	flushLikeCounts(stopCtx, context.Background(), es, coalescer, nil, 1, false, quietLogger())

	pending, _ := coalescer.Stats()
	if written.Load() != 100 {
		t.Errorf("wrote %d posts, want exactly the 100 in flight when the flush was stopped", written.Load())
	}
	if got := written.Load() + int64(pending); got != 1000 {
		t.Errorf("written (%d) + held (%d) = %d, want all 1000 accounted for", written.Load(), pending, got)
	}
}

// The final flush must fit in the drain: a prod flush holds up to ~250
// chunks at ~50ms each, 10-25s written one at a time against a 5s deadline,
// which is how every prod deploy was losing its held counts. What makes it fit
// is writing shutdownFlushConcurrency chunks at once, so that is what this
// asserts, rather than a wall-clock time that a loaded CI runner can miss.
//
// Each posts request is held until shutdownFlushConcurrency of them are in
// flight together. A flush that writes fewer at a time never gets there, and
// the requests give up after a timeout instead.
func TestShutdownFlushWritesChunksConcurrently(t *testing.T) {
	var inFlight atomic.Int64
	allInFlight := make(chan struct{})
	var once sync.Once
	es, written := likeCountES(t, func(int) {
		defer inFlight.Add(-1)
		if inFlight.Add(1) == shutdownFlushConcurrency {
			once.Do(func() { close(allInFlight) })
		}
		select {
		case <-allInFlight:
		case <-time.After(time.Second):
		}
	})
	// One chunk per writer, so all of them must be in flight at once.
	coalescer := heldCoalescer(100 * shutdownFlushConcurrency)

	flushLikeCounts(context.Background(), context.Background(), es, coalescer, nil, shutdownFlushConcurrency, false, quietLogger())

	select {
	case <-allInFlight:
	default:
		t.Fatalf("never had %d chunks in flight at once; the shutdown flush is not concurrent", shutdownFlushConcurrency)
	}
	if pending, _ := coalescer.Stats(); written.Load() != int64(100*shutdownFlushConcurrency) || pending != 0 {
		t.Errorf("wrote %d of %d posts with %d still held, want all written", written.Load(), 100*shutdownFlushConcurrency, pending)
	}
}
