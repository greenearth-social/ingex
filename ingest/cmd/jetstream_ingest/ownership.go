package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/jetstream_ingest"
)

// ingestionLease separates the ownership loop from GCS operations so tests can
// exercise contention and renewal failures without a storage service.
type ingestionLease interface {
	TryAcquire(context.Context, time.Duration) (bool, error)
	Renew(context.Context, time.Duration) error
	Release(context.Context) error
	ExpiresAt() time.Time
}

// ttl is the ownership lifetime confirmed by acquisition or renewal; renew is
// the renewal interval, and poll is the delay between acquisition attempts.
// request bounds each storage call, margin stops work before ownership expires,
// and drain bounds the time allowed to finish work during graceful shutdown.
type leaseTiming struct {
	ttl, renew, poll, request, margin, drain time.Duration
}

var ingestionLeaseTiming = leaseTiming{
	ttl: time.Minute, renew: 20 * time.Second, poll: 5 * time.Second,
	request: 2 * time.Second, margin: 10 * time.Second, drain: 8 * time.Second,
}

func withIngestionLease(ctx context.Context, statePath string, logger *common.IngestLogger, health *common.HealthServer, run func(context.Context, context.Context) error) error {
	if !strings.HasPrefix(statePath, "gs://") {
		// Local state assumes one process. Keep work alive after intake stops;
		// runIngestion bounds its own drain and saves the final cursor.
		err := run(ctx, context.WithoutCancel(ctx))
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	parts := strings.SplitN(strings.TrimPrefix(statePath, "gs://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid GCS state path: %s", statePath)
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	lease := jetstream_ingest.NewLease(client, parts[0], parts[1]+".lease", uuid.NewString())
	return runWithLease(ctx, lease, ingestionLeaseTiming, logger, func() {
		// Standby is a healthy role: the instance can take over when the owner leaves.
		health.SetHealthy(true, "Waiting for ingestion lease")
	}, run)
}

func runWithLease(ctx context.Context, lease ingestionLease, timing leaseTiming, logger *common.IngestLogger, waiting func(), run func(context.Context, context.Context) error) error {
	for ctx.Err() == nil {
		waiting()
		requestCtx, cancel := context.WithTimeout(ctx, timing.request)
		acquired, err := lease.TryAcquire(requestCtx, timing.ttl)
		cancel()
		if err != nil {
			logger.Error("Failed to acquire ingestion lease: %v", err)
		}
		// A slow acquisition can consume most of the lease. Start ingestion only
		// while there is still time before the safety margin begins.
		if acquired && time.Until(lease.ExpiresAt()) > timing.margin && ctx.Err() == nil {
			logger.Info("Acquired ingestion lease")
			if err := runLeaseSession(ctx, lease, timing, logger, run); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(timing.poll):
		}
	}
	return nil
}

// Intake stops immediately on SIGTERM; existing work gets a bounded drain while
// the lease is still renewed. Losing the lease cancels both contexts immediately.
// The callback receives intakeCtx first and workCtx second. It must wait for all
// writers to finish before returning, including when cancellation requests a stop.
func runLeaseSession(ctx context.Context, lease ingestionLease, timing leaseTiming, logger *common.IngestLogger, run func(context.Context, context.Context) error) error {
	// WithoutCancel keeps process shutdown from canceling existing work before
	// it can drain. Intake and renewal are children of work, so cancelWork signals both to stop.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	intakeCtx, cancelIntake := context.WithCancel(workCtx)
	defer cancelIntake()
	// finished signals that the ingestion callback has returned; shutdownDone
	// lets us wait for the shutdown watcher itself to finish.
	finished := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-finished:
			return
		case <-ctx.Done():
			cancelIntake()
		}
		timer := time.NewTimer(timing.drain)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			cancelWork()
		}
	}()

	// Stop before the confirmed expiry even if a renewal request stalls.
	expiry := time.AfterFunc(time.Until(lease.ExpiresAt())-timing.margin, cancelWork)
	defer expiry.Stop()
	renewCtx, stopRenewing := context.WithCancel(workCtx)
	renewDone := make(chan struct{})
	lost := false
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(timing.renew)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				requestCtx, cancel := context.WithTimeout(renewCtx, timing.request)
				err := lease.Renew(requestCtx, timing.ttl)
				cancel()
				if err != nil {
					if renewCtx.Err() != nil {
						return
					}
					logger.Error("Stopping ingestion after lease renewal failed: %v", err)
					// An unconfirmed renewal cannot justify continued writes, so
					// lease failure stops work without a graceful drain allowance.
					lost = true
					cancelWork()
					return
				}
				// Only a confirmed renewal can move the watchdog deadline forward.
				expiry.Reset(time.Until(lease.ExpiresAt()) - timing.margin)
			}
		}
	}()

	err := run(intakeCtx, workCtx)
	close(finished)
	stopRenewing()
	// Cancellation requests a stop; receiving from the closed channel confirms
	// completion. Join renewal before reading lost or releasing ownership so a
	// renewal cannot race with the release.
	<-renewDone
	<-shutdownDone
	// Ingestion and its checkpoint writer have stopped. Release gets a fresh,
	// bounded context because the process shutdown context may already be canceled.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.request)
	defer cancel()
	if releaseErr := lease.Release(releaseCtx); releaseErr != nil && !errors.Is(releaseErr, jetstream_ingest.ErrLeaseLost) {
		logger.Error("Failed to release ingestion lease: %v", releaseErr)
	}
	// Ownership loss returns to the acquisition loop; process shutdown ends it.
	// Other ingestion errors propagate to the caller.
	if ctx.Err() != nil || lost || workCtx.Err() != nil {
		return nil
	}
	return err
}
