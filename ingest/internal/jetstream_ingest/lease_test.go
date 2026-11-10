package jetstream_ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// leaseStorage exercises the real GCS client and rejects writes without a
// matching generation, including two writers that read the same expired lease.
type leaseStorage struct {
	mu          sync.Mutex
	data        []byte
	generation  int64
	writeStatus int
	beforeWrite func()
	reads       *sync.WaitGroup
	// The next committed write reports failure anyway: lostResponse as a
	// connection error, or committedConflict as the 412 a client retry receives.
	lostResponse      error
	committedConflict bool
}

func (s *leaseStorage) RoundTrip(r *http.Request) (*http.Response, error) {
	response := func(code int, body []byte, headers http.Header) (*http.Response, error) {
		if headers == nil {
			headers = http.Header{"Content-Type": {"application/json"}}
		}
		return &http.Response{
			StatusCode: code, Header: headers, Body: io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)), Request: r,
		}, nil
	}
	errorResponse := func(code int) (*http.Response, error) {
		return response(code, []byte(fmt.Sprintf(`{"error":{"code":%d,"message":"test storage error"}}`, code)), nil)
	}
	if r.Method == http.MethodGet {
		s.mu.Lock()
		data := append([]byte(nil), s.data...)
		generation := s.generation
		reads := s.reads
		s.mu.Unlock()
		if reads != nil {
			// Both requests capture their snapshot before either may attempt a write.
			// This forces the race instead of relying on goroutine scheduling.
			reads.Done()
			reads.Wait()
		}
		if generation == 0 {
			return errorResponse(http.StatusNotFound)
		}
		return response(http.StatusOK, data, http.Header{
			"Content-Type":      {"application/json"},
			"Content-Length":    {strconv.Itoa(len(data))},
			"X-Goog-Generation": {strconv.FormatInt(generation, 10)},
		})
	}
	if r.Method != http.MethodPost {
		return errorResponse(http.StatusMethodNotAllowed)
	}
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	parts := multipart.NewReader(r.Body, params["boundary"])
	metadata, err := parts.NextPart()
	if err != nil {
		return nil, err
	}
	_ = metadata.Close()
	body, err := parts.NextPart()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	match, err := strconv.ParseInt(r.URL.Query().Get("ifGenerationMatch"), 10, 64)
	if err != nil {
		return errorResponse(http.StatusBadRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeStatus != 0 {
		return errorResponse(s.writeStatus)
	}
	if match != s.generation {
		return errorResponse(http.StatusPreconditionFailed)
	}
	if s.beforeWrite != nil {
		s.beforeWrite()
	}
	s.generation++
	s.data = data
	if err := s.lostResponse; err != nil {
		s.lostResponse = nil
		return nil, err
	}
	if s.committedConflict {
		s.committedConflict = false
		return errorResponse(http.StatusPreconditionFailed)
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	return response(http.StatusOK, []byte(fmt.Sprintf(`{"generation":"%d","bucket":"test","name":"lease.json","crc32c":"%s"}`,
		s.generation, base64.StdEncoding.EncodeToString(checksum[:]))), nil)
}

func newTestLease(t *testing.T, store *leaseStorage, owner string, now func() time.Time) *Lease {
	t.Helper()
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint("http://lease.test/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: store}),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	lease := NewLease(client, "test", "lease.json", owner)
	lease.now = now
	return lease
}

func acquireLease(t *testing.T, lease *Lease) {
	t.Helper()
	if acquired, err := lease.TryAcquire(context.Background(), time.Minute); err != nil || !acquired {
		t.Fatalf("acquire lease: acquired=%t, err=%v", acquired, err)
	}
}

func TestLeaseWaitsForOwnerAndAcquiresAfterRelease(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	clock := func() time.Time { return now }
	a := newTestLease(t, store, "a", clock)
	b := newTestLease(t, store, "b", clock)
	acquireLease(t, a)
	if acquired, err := b.TryAcquire(context.Background(), time.Minute); err != nil || acquired {
		t.Fatalf("newcomer must wait: acquired=%t, err=%v", acquired, err)
	}
	now = now.Add(20 * time.Second)
	if err := a.Renew(context.Background(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if !a.ExpiresAt().Equal(now.Add(time.Minute)) {
		t.Fatalf("renewed expiration = %s", a.ExpiresAt())
	}
	if err := a.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	acquireLease(t, b)
	if err := a.Renew(context.Background(), time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("released owner renewed: %v", err)
	}
}

func TestLeaseConcurrentAcquisitionHasOneWinner(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			now := time.Now()
			store := &leaseStorage{}
			if existing {
				store.generation = 7
				store.data, _ = json.Marshal(leaseRecord{OwnerID: "expired", ExpiresAt: now.Add(-time.Minute)})
			}
			store.reads = &sync.WaitGroup{}
			store.reads.Add(2)
			clock := func() time.Time { return now }
			a := newTestLease(t, store, "a", clock)
			b := newTestLease(t, store, "b", clock)
			type result struct {
				acquired bool
				err      error
			}
			results := make(chan result, 2)
			for _, lease := range []*Lease{a, b} {
				go func() {
					acquired, err := lease.TryAcquire(context.Background(), time.Minute)
					results <- result{acquired, err}
				}()
			}
			winners := 0
			for range 2 {
				result := <-results
				if result.err != nil {
					t.Fatal(result.err)
				}
				if result.acquired {
					winners++
				}
			}
			if winners != 1 {
				t.Fatalf("got %d winners, want 1", winners)
			}
		})
	}
}

func TestLeaseExpiresAndRejectsStaleOwner(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	clock := func() time.Time { return now }
	a := newTestLease(t, store, "a", clock)
	b := newTestLease(t, store, "b", clock)
	acquireLease(t, a)
	// Simulate a crashed owner: its lease expires without an explicit release.
	now = now.Add(time.Minute)
	if err := a.Renew(context.Background(), time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner renewed: %v", err)
	}
	acquireLease(t, b)
	if err := a.Release(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner released successor lease: %v", err)
	}
	if err := b.Renew(context.Background(), time.Minute); err != nil {
		t.Fatalf("successor lease was affected by stale owner: %v", err)
	}
}

func TestLeaseRenewalRejectsChangedGeneration(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	lease := newTestLease(t, store, "a", func() time.Time { return now })
	acquireLease(t, lease)
	// Simulate another writer replacing the object before this owner renews.
	store.generation++
	if err := lease.Renew(context.Background(), time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renewal should reject changed generation: %v", err)
	}
}

func TestLeaseFailedWritesDoNotGrantOrExtendOwnership(t *testing.T) {
	store := &leaseStorage{writeStatus: http.StatusForbidden}
	now := time.Now()
	lease := newTestLease(t, store, "a", func() time.Time { return now })
	if acquired, err := lease.TryAcquire(context.Background(), time.Minute); err == nil || acquired {
		t.Fatalf("failed write granted ownership: acquired=%t, err=%v", acquired, err)
	}
	if !lease.ExpiresAt().IsZero() {
		t.Fatal("failed acquisition set an expiration")
	}
	store.writeStatus = 0
	acquireLease(t, lease)
	expiresAt := lease.ExpiresAt()
	now = now.Add(20 * time.Second)
	store.writeStatus = http.StatusForbidden
	if err := lease.Renew(context.Background(), time.Minute); err == nil {
		t.Fatal("failed renewal returned no error")
	}
	if !lease.ExpiresAt().Equal(expiresAt) {
		t.Fatal("failed renewal extended the local expiration")
	}
}

func TestLeaseExpirationIncludesUploadTime(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	startedAt := now
	lease := newTestLease(t, store, "a", func() time.Time { return now })
	// Advance the fake clock during upload, without making the test sleep.
	store.beforeWrite = func() { now = now.Add(10 * time.Second) }
	acquireLease(t, lease)
	if !lease.ExpiresAt().Equal(startedAt.Add(time.Minute)) {
		t.Fatalf("upload delay extended the local expiration: %s", lease.ExpiresAt())
	}
}

func TestLeaseSlowAcquisitionDoesNotGrantExpiredOwnership(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	lease := newTestLease(t, store, "a", func() time.Time { return now })
	store.beforeWrite = func() { now = now.Add(time.Minute) }
	acquired, err := lease.TryAcquire(context.Background(), time.Minute)
	if acquired || !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("slow acquisition granted expired ownership: acquired=%t, err=%v", acquired, err)
	}
}

func TestLeaseReleaseAfterUncertainRenewalFreesLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(*leaseStorage)
	}{
		// The renewal committed, but the connection dropped before the response.
		{name: "lost response", fail: func(s *leaseStorage) { s.lostResponse = errors.New("response lost") }},
		// The renewal never reached storage.
		{name: "rejected write", fail: func(s *leaseStorage) { s.writeStatus = http.StatusForbidden }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &leaseStorage{}
			now := time.Now()
			owner := newTestLease(t, store, "a", func() time.Time { return now })
			acquireLease(t, owner)
			acquiredExpiry := owner.ExpiresAt()
			now = now.Add(20 * time.Second)
			tc.fail(store)
			if err := owner.Renew(context.Background(), time.Minute); err == nil || errors.Is(err, ErrLeaseLost) {
				t.Fatalf("expected an uncertain failure: %v", err)
			}
			if !owner.ExpiresAt().Equal(acquiredExpiry) {
				t.Fatal("unconfirmed renewal extended the local expiration")
			}
			store.writeStatus = 0
			if err := owner.Release(context.Background()); err != nil {
				t.Fatalf("release after uncertain renewal: %v", err)
			}
			// A successor acquires at once instead of waiting out the ttl.
			acquireLease(t, newTestLease(t, store, "b", func() time.Time { return now }))
		})
	}
}

func TestLeaseRenewalRecoversClientRetryConflict(t *testing.T) {
	store := &leaseStorage{}
	now := time.Now()
	lease := newTestLease(t, store, "a", func() time.Time { return now })
	acquireLease(t, lease)
	now = now.Add(20 * time.Second)
	// A client-level retry after a dropped connection sees its own commit as a 412.
	// Reading the record confirms the write, so ownership is not in doubt.
	store.committedConflict = true
	if err := lease.Renew(context.Background(), time.Minute); err != nil {
		t.Fatalf("own committed write should not lose the lease: %v", err)
	}
	if want := now.Add(time.Minute); !lease.ExpiresAt().Equal(want) {
		t.Fatalf("expiry = %v, want %v", lease.ExpiresAt(), want)
	}
	if err := lease.Renew(context.Background(), time.Minute); err != nil {
		t.Fatalf("renewal after recovery: %v", err)
	}
}

func TestLeaseAfterUncertainRenewalLeavesOtherWritersAlone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner string
		// offset moves the stored deadline away from every renewal this owner tried.
		offset time.Duration
	}{
		{name: "other owner", owner: "b"},
		{name: "unattempted deadline", owner: "a", offset: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &leaseStorage{}
			now := time.Now()
			lease := newTestLease(t, store, "a", func() time.Time { return now })
			acquireLease(t, lease)
			now = now.Add(20 * time.Second)
			store.writeStatus = http.StatusForbidden
			if err := lease.Renew(context.Background(), time.Minute); err == nil || errors.Is(err, ErrLeaseLost) {
				t.Fatalf("expected an uncertain failure: %v", err)
			}
			store.writeStatus = 0
			// Replace the record as a different writer would.
			data, err := json.Marshal(leaseRecord{OwnerID: tc.owner, ExpiresAt: now.Add(time.Minute + tc.offset)})
			if err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			store.data = data
			store.generation++
			generation := store.generation
			store.mu.Unlock()
			if err := lease.Release(context.Background()); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("release should report the lease lost: %v", err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.generation != generation || !bytes.Equal(store.data, data) {
				t.Fatal("release overwrote a record this owner did not write")
			}
		})
	}
}
