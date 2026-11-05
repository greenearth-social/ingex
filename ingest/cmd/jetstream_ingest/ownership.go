package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/greenearth/ingest/internal/common"
	"github.com/greenearth/ingest/internal/jetstream_ingest"
)

type ingestionLease interface {
	TryAcquire(context.Context, time.Duration) (bool, error)
	Renew(context.Context, time.Duration) error
	Release(context.Context) error
	ExpiresAt() time.Time
}

type leaseTiming struct {
	ttl, renew, poll, request, margin, drain time.Duration
}

var ingestionLeaseTiming = leaseTiming{
	ttl: time.Minute, renew: 20 * time.Second, poll: 5 * time.Second,
	request: 2 * time.Second, margin: 10 * time.Second, drain: 8 * time.Second,
}

func withIngestionLease(ctx context.Context, statePath string, logger *common.IngestLogger, health *common.HealthServer, run func(context.Context, context.Context) error) error {
	var lease ingestionLease
	if strings.HasPrefix(statePath, "gs://") {
		parts := strings.SplitN(strings.TrimPrefix(statePath, "gs://"), "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid GCS state path: %s", statePath)
		}
		client, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = client.Close() }()
		lease = jetstream_ingest.NewLease(client, parts[0], parts[1]+".lease", uuid.NewString())
	} else {
		// Local development uses a process lock, released by the OS on exit.
		file, err := os.OpenFile(statePath+".lease", os.O_CREATE|os.O_RDWR, 0600) // #nosec G304 - statePath is controlled service configuration.
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		lease = &localLease{file: file}
	}
	return runWithLease(ctx, lease, ingestionLeaseTiming, logger, func() {
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
// The callback must join its writers before returning so release is safe.
func runLeaseSession(ctx context.Context, lease ingestionLease, timing leaseTiming, logger *common.IngestLogger, run func(context.Context, context.Context) error) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	intakeCtx, cancelIntake := context.WithCancel(workCtx)
	defer cancelIntake()
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
					lost = true
					cancelWork()
					return
				}
				expiry.Reset(time.Until(lease.ExpiresAt()) - timing.margin)
			}
		}
	}()

	err := run(intakeCtx, workCtx)
	close(finished)
	stopRenewing()
	<-renewDone
	<-shutdownDone
	// Only release after ingestion and its checkpoint writer have stopped.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.request)
	defer cancel()
	if releaseErr := lease.Release(releaseCtx); releaseErr != nil && !errors.Is(releaseErr, jetstream_ingest.ErrLeaseLost) {
		logger.Error("Failed to release ingestion lease: %v", releaseErr)
	}
	if ctx.Err() != nil || lost || workCtx.Err() != nil {
		return nil
	}
	return err
}

type localLease struct {
	file    *os.File
	expires time.Time
}

func (l *localLease) TryAcquire(ctx context.Context, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) // #nosec G115 - OS file descriptors fit in a POSIX int.
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	l.expires = time.Now().Add(ttl)
	return true, nil
}

func (l *localLease) Renew(ctx context.Context, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.expires = time.Now().Add(ttl)
	return nil
}

func (l *localLease) Release(context.Context) error {
	return syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN) // #nosec G115 - OS file descriptors fit in a POSIX int.
}

func (l *localLease) ExpiresAt() time.Time { return l.expires }
