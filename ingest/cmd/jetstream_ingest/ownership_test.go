package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/jetstream_ingest"
)

type testIngestionLease struct {
	mu      sync.Mutex
	expires time.Time
	acquire func(context.Context, time.Duration) (bool, error)
	renew   func(context.Context, time.Duration) error
	release func(context.Context) error
}

func (l *testIngestionLease) TryAcquire(ctx context.Context, ttl time.Duration) (bool, error) {
	return l.acquire(ctx, ttl)
}

func (l *testIngestionLease) Renew(ctx context.Context, ttl time.Duration) error {
	if l.renew != nil {
		return l.renew(ctx, ttl)
	}
	l.mu.Lock()
	l.expires = time.Now().Add(ttl)
	l.mu.Unlock()
	return nil
}

func (l *testIngestionLease) Release(ctx context.Context) error {
	if l.release != nil {
		return l.release(ctx)
	}
	return nil
}

func (l *testIngestionLease) ExpiresAt() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expires
}

func testLeaseTiming() leaseTiming {
	return leaseTiming{
		ttl: time.Second, renew: 10 * time.Millisecond, poll: 5 * time.Millisecond,
		request: 100 * time.Millisecond, margin: 100 * time.Millisecond, drain: 500 * time.Millisecond,
	}
}

func waitForOwnershipEvent(t *testing.T, event <-chan struct{}) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ownership event")
	}
}

func waitForOwnershipResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("ownership runner returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ownership runner did not stop")
	}
}

func TestRunWithLeaseWaiterStaysAvailableWithoutRunningIngestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := make(chan struct{}, 10)
	var waitingCalls atomic.Int32
	var runs atomic.Int32
	lease := &testIngestionLease{
		acquire: func(context.Context, time.Duration) (bool, error) {
			attempts <- struct{}{}
			return false, nil
		},
	}
	result := make(chan error, 1)
	go func() {
		result <- runWithLease(ctx, lease, testLeaseTiming(), common.NewLogger(false), func() {
			// The production callback marks the health endpoint healthy while waiting.
			waitingCalls.Add(1)
		}, func(context.Context, context.Context) error {
			runs.Add(1)
			return nil
		})
	}()
	for range 3 {
		waitForOwnershipEvent(t, attempts)
	}
	if waitingCalls.Load() < 3 || runs.Load() != 0 {
		t.Fatalf("waiting callbacks = %d, ingestion runs = %d", waitingCalls.Load(), runs.Load())
	}
	select {
	case err := <-result:
		t.Fatalf("standby exited before shutdown: %v", err)
	default:
	}
	cancel()
	waitForOwnershipResult(t, result)
}

func TestRunLeaseSessionShutdownDrainsBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	intakeStopped := make(chan struct{})
	renewedDuringDrain := make(chan struct{}, 1)
	finishCleanup := make(chan struct{})
	defer func() {
		select {
		case <-finishCleanup:
		default:
			close(finishCleanup)
		}
	}()
	released := make(chan struct{})
	var cleanupComplete atomic.Bool
	lease := &testIngestionLease{expires: time.Now().Add(time.Second)}
	lease.renew = func(context.Context, time.Duration) error {
		if ctx.Err() != nil {
			select {
			case renewedDuringDrain <- struct{}{}:
			default:
			}
		}
		return nil
	}
	lease.release = func(releaseCtx context.Context) error {
		if !cleanupComplete.Load() {
			t.Error("released ownership before callback cleanup completed")
		}
		if releaseCtx.Err() != nil {
			t.Errorf("release inherited the canceled shutdown context: %v", releaseCtx.Err())
		}
		close(released)
		return nil
	}
	result := make(chan error, 1)
	go func() {
		result <- runLeaseSession(ctx, lease, testLeaseTiming(), common.NewLogger(false), func(intake, work context.Context) error {
			close(started)
			<-intake.Done()
			if work.Err() != nil {
				t.Error("work was canceled before the graceful drain")
			}
			close(intakeStopped)
			<-finishCleanup
			if work.Err() != nil {
				t.Error("work was canceled while cleanup was within the drain allowance")
			}
			cleanupComplete.Store(true)
			return intake.Err()
		})
	}()
	waitForOwnershipEvent(t, started)
	cancel()
	waitForOwnershipEvent(t, intakeStopped)
	waitForOwnershipEvent(t, renewedDuringDrain)
	select {
	case <-released:
		t.Fatal("released ownership while callback was still draining")
	default:
	}
	close(finishCleanup)
	waitForOwnershipResult(t, result)
	waitForOwnershipEvent(t, released)
}

func TestRunWithLeaseRenewalFailureStopsWritersAndReturnsToWaiting(t *testing.T) {
	for _, renewErr := range []error{jetstream_ingest.ErrLeaseLost, errors.New("storage unavailable")} {
		t.Run(renewErr.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waitingAgain := make(chan struct{}, 1)
			var acquisitions atomic.Int32
			var runs atomic.Int32
			var writersStopped atomic.Bool
			lease := &testIngestionLease{expires: time.Now().Add(time.Second)}
			lease.acquire = func(context.Context, time.Duration) (bool, error) {
				if acquisitions.Add(1) == 1 {
					return true, nil
				}
				select {
				case waitingAgain <- struct{}{}:
				default:
				}
				return false, nil
			}
			lease.renew = func(context.Context, time.Duration) error { return renewErr }
			lease.release = func(context.Context) error {
				if !writersStopped.Load() {
					t.Error("release happened before writers stopped after lease loss")
				}
				return jetstream_ingest.ErrLeaseLost
			}
			result := make(chan error, 1)
			go func() {
				result <- runWithLease(ctx, lease, testLeaseTiming(), common.NewLogger(false), func() {}, func(intake, work context.Context) error {
					runs.Add(1)
					<-work.Done()
					// Parent cancellation can wake this goroutine before it reaches children.
					select {
					case <-intake.Done():
					case <-time.After(time.Second):
						t.Error("lease loss canceled work but left intake running")
					}
					writersStopped.Store(true)
					return work.Err()
				})
			}()
			waitForOwnershipEvent(t, waitingAgain)
			if !writersStopped.Load() || runs.Load() != 1 || ctx.Err() != nil {
				t.Fatalf("stopped = %v, runs = %d, parent error = %v", writersStopped.Load(), runs.Load(), ctx.Err())
			}
			select {
			case err := <-result:
				t.Fatalf("lease loss exited instead of waiting: %v", err)
			default:
			}
			cancel()
			waitForOwnershipResult(t, result)
		})
	}
}

func TestRunLeaseSessionBoundsGracefulDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timing := testLeaseTiming()
	timing.drain = 40 * time.Millisecond
	started := make(chan struct{})
	lease := &testIngestionLease{expires: time.Now().Add(time.Second)}
	result := make(chan error, 1)
	go func() {
		result <- runLeaseSession(ctx, lease, timing, common.NewLogger(false), func(intake, work context.Context) error {
			close(started)
			<-intake.Done()
			<-work.Done()
			return work.Err()
		})
	}()
	waitForOwnershipEvent(t, started)
	cancel()
	waitForOwnershipResult(t, result)
}

func TestRunLeaseSessionExpiryCancelsStalledRenewal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timing := testLeaseTiming()
	timing.request = 5 * time.Second
	timing.margin = 50 * time.Millisecond
	lease := &testIngestionLease{expires: time.Now().Add(150 * time.Millisecond)}
	renewStarted := make(chan struct{})
	lease.renew = func(renewCtx context.Context, _ time.Duration) error {
		close(renewStarted)
		<-renewCtx.Done()
		return renewCtx.Err()
	}
	result := make(chan error, 1)
	go func() {
		result <- runLeaseSession(ctx, lease, timing, common.NewLogger(false), func(intake, work context.Context) error {
			<-work.Done()
			select {
			case <-intake.Done():
			case <-time.After(time.Second):
				t.Error("expiry canceled work but left intake running")
			}
			if ctx.Err() != nil {
				t.Errorf("expiry canceled the parent context: %v", ctx.Err())
			}
			return work.Err()
		})
	}()
	waitForOwnershipEvent(t, renewStarted)
	// The watchdog must stop work without waiting for the five-second request timeout.
	waitForOwnershipResult(t, result)
}

func TestLocalLeaseContentionAndHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jetstream_state.json.lease")
	openLease := func() *localLease {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		return &localLease{file: file}
	}
	first, second := openLease(), openLease()
	ctx := context.Background()
	if acquired, err := first.TryAcquire(ctx, time.Second); err != nil || !acquired {
		t.Fatalf("first acquisition = %v, %v", acquired, err)
	}
	if acquired, err := second.TryAcquire(ctx, time.Second); err != nil || acquired {
		t.Fatalf("contending acquisition = %v, %v", acquired, err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if acquired, err := second.TryAcquire(ctx, time.Second); err != nil || !acquired {
		t.Fatalf("successor acquisition = %v, %v", acquired, err)
	}
	if err := second.Renew(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	if acquired, err := first.TryAcquire(ctx, time.Second); err != nil || acquired {
		t.Fatalf("previous owner reacquisition = %v, %v", acquired, err)
	}
}
