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

// Hooks let each test control ownership failures independently of storage.
// The mutex protects expiry updates made by the renewal goroutine.
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
		request: 100 * time.Millisecond, retry: 10 * time.Millisecond, margin: 100 * time.Millisecond, drain: 500 * time.Millisecond,
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
	// Hold simulated writer cleanup open so the test can check renewal and
	// confirm that ownership has not been released while cleanup is unfinished.
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
	// Explicit ownership loss stops writers at once. Storage failures that never
	// clear stop them when the last confirmed lease nears expiry. Either way the
	// still-running process waits for ownership again.
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
	// Successful renewals keep ownership alive. Only the drain deadline should
	// end the callback that deliberately waits for work cancellation below.
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

func TestWithIngestionLeaseLocalPathRunsWithoutLease(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "jetstream_state.json")
	wantErr := errors.New("ingestion failed")
	runs := 0
	// Local ingestion has no standby role, so it does not need a health server.
	err := withIngestionLease(context.Background(), statePath, common.NewLogger(false), nil, func(context.Context, context.Context) error {
		runs++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ingestion error = %v, want %v", err, wantErr)
	}
	if runs != 1 {
		t.Fatalf("ingestion runs = %d, want 1", runs)
	}
	if _, err := os.Stat(statePath + ".lease"); !os.IsNotExist(err) {
		t.Fatalf("local ingestion created a lease file or could not check its absence: %v", err)
	}
}

func TestWithIngestionLeaseLocalCancellationIsNormalShutdown(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "jetstream_state.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := withIngestionLease(ctx, statePath, common.NewLogger(false), nil, func(intakeCtx, _ context.Context) error {
		// Startup may return cancellation before ingestion reaches its normal drain path.
		cancel()
		return intakeCtx.Err()
	})
	if ctx.Err() != context.Canceled {
		t.Fatal("ingestion callback did not cancel the process context")
	}
	if err != nil {
		t.Fatalf("normal shutdown returned an error: %v", err)
	}
}

func TestRunLeaseSessionRetriesUncertainRenewalFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease := &testIngestionLease{expires: time.Now().Add(time.Second)}
	var attempts atomic.Int32
	renewed := make(chan struct{}, 1)
	lease.renew = func(_ context.Context, ttl time.Duration) error {
		// Fail the first two attempts the way a dropped GCS connection does.
		if attempts.Add(1) <= 2 {
			return errors.New("connection reset by peer")
		}
		lease.mu.Lock()
		lease.expires = time.Now().Add(ttl)
		lease.mu.Unlock()
		select {
		case renewed <- struct{}{}:
		default:
		}
		return nil
	}
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runLeaseSession(ctx, lease, testLeaseTiming(), common.NewLogger(false), func(intake, work context.Context) error {
			close(started)
			<-intake.Done()
			if work.Err() != nil {
				t.Error("renewal retries canceled work before shutdown")
			}
			return nil
		})
	}()
	waitForOwnershipEvent(t, started)
	waitForOwnershipEvent(t, renewed)
	// Outlive the original lease to show the recovered renewal moved the watchdog.
	time.Sleep(time.Second)
	select {
	case err := <-result:
		t.Fatalf("session ended despite a confirmed renewal: %v", err)
	default:
	}
	cancel()
	waitForOwnershipResult(t, result)
}
