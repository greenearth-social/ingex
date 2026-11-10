package jetstream_ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// ErrLeaseLost means this instance can no longer use its acquired lease.
var ErrLeaseLost = errors.New("ingestion lease lost")

// leaseRecord is shared in GCS; Lease below keeps this process's confirmed state.
type leaseRecord struct {
	OwnerID   string    `json:"owner_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Lease coordinates one active ingester using GCS generation preconditions.
// Calls must be serialized; the worker must stop before ExpiresAt if renewal fails.
// ownerID identifies the process for inspection; generation preconditions decide ownership.
type Lease struct {
	object     *storage.ObjectHandle
	ownerID    string
	generation int64
	expiresAt  time.Time
	// unconfirmed holds deadlines from renewal writes whose outcome is unknown.
	// GCS may have committed one even though the response never arrived.
	unconfirmed []time.Time
	now         func() time.Time
}

// NewLease uses one GCS object to coordinate ownership among instances.
func NewLease(client *storage.Client, bucket, object, ownerID string) *Lease {
	return &Lease{
		object:  client.Bucket(bucket).Object(object),
		ownerID: ownerID,
		now:     time.Now,
	}
}

// TryAcquire returns false when another instance owns the lease or wins the race
// to acquire it. It never changes an unexpired lease held by another instance.
func (l *Lease) TryAcquire(ctx context.Context, ttl time.Duration) (bool, error) {
	// Creation and takeover both use a conditional write so only one contender wins.
	conditions := storage.Conditions{DoesNotExist: true}
	// NewReader supplies the body and generation from the same response.
	reader, err := l.object.NewReader(ctx)
	if err == nil {
		var record leaseRecord
		decodeErr := json.NewDecoder(reader).Decode(&record)
		generation := reader.Attrs.Generation
		_ = reader.Close()
		if decodeErr != nil {
			return false, fmt.Errorf("decode ingestion lease: %w", decodeErr)
		}
		if l.now().Before(record.ExpiresAt) {
			return false, nil
		}
		conditions = storage.Conditions{GenerationMatch: generation}
	} else if !errors.Is(err, storage.ErrObjectNotExist) {
		return false, fmt.Errorf("read ingestion lease: %w", err)
	}

	// Count upload time against the lease lifetime rather than extending local
	// permission beyond the expiration written to GCS.
	expiresAt := l.now().Add(ttl)
	generation, err := l.write(ctx, conditions, expiresAt)
	if leasePreconditionFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Remember ownership only after GCS confirms the write.
	l.generation = generation
	l.expiresAt = expiresAt
	l.unconfirmed = nil
	// A delayed response can arrive after the permission it granted has expired.
	if !l.now().Before(expiresAt) {
		return false, ErrLeaseLost
	}
	return true, nil
}

// Renew extends only the generation acquired by this instance. An expired owner
// must acquire again instead of reviving its old permission to process events.
func (l *Lease) Renew(ctx context.Context, ttl time.Duration) error {
	if l.generation == 0 || !l.now().Before(l.expiresAt) {
		return ErrLeaseLost
	}
	expiresAt := l.now().Add(ttl)
	generation, err := l.write(ctx, storage.Conditions{GenerationMatch: l.generation}, expiresAt)
	if leasePreconditionFailed(err) {
		// The generation moved. That is another owner, or one of our own writes
		// that committed after its response was lost (including a client retry of
		// this request). Only the second case keeps ownership.
		l.unconfirmed = append(l.unconfirmed, expiresAt)
		ownGeneration, ownExpiresAt, readErr := l.findOwnRenewal(ctx, l.unconfirmed)
		if readErr != nil {
			return readErr
		}
		if ownGeneration == 0 {
			l.generation = 0
			l.unconfirmed = nil
			return ErrLeaseLost
		}
		generation, expiresAt = ownGeneration, ownExpiresAt
	} else if err != nil {
		l.unconfirmed = append(l.unconfirmed, expiresAt)
		return err
	}
	// Keep the previous deadline unless GCS confirms the renewal write.
	l.generation = generation
	l.expiresAt = expiresAt
	l.unconfirmed = nil
	if !l.now().Before(expiresAt) {
		return ErrLeaseLost
	}
	return nil
}

// findOwnRenewal returns the stored generation and deadline when the record is
// one of this owner's renewal writes, or zero when someone else wrote it. Each
// renewal writes a fresh deadline, so a match identifies a write this instance
// made against its own generation, which only GCS could have accepted.
func (l *Lease) findOwnRenewal(ctx context.Context, attempts []time.Time) (int64, time.Time, error) {
	reader, err := l.object.NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return 0, time.Time{}, nil
	}
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("read ingestion lease: %w", err)
	}
	var record leaseRecord
	decodeErr := json.NewDecoder(reader).Decode(&record)
	generation := reader.Attrs.Generation
	_ = reader.Close()
	if decodeErr != nil {
		return 0, time.Time{}, fmt.Errorf("decode ingestion lease: %w", decodeErr)
	}
	// A renewal always extends the confirmed deadline, so anything else is not one.
	if record.OwnerID != l.ownerID || !record.ExpiresAt.After(l.expiresAt) {
		return 0, time.Time{}, nil
	}
	for _, attempt := range attempts {
		if record.ExpiresAt.Equal(attempt) {
			return generation, record.ExpiresAt, nil
		}
	}
	return 0, time.Time{}, nil
}

// Release makes the lease available after ingestion and cursor persistence stop.
// The generation check prevents an old owner from releasing its successor's lease.
func (l *Lease) Release(ctx context.Context) error {
	if l.generation == 0 {
		return ErrLeaseLost
	}
	// Expiring the record allows immediate takeover without deleting the object.
	_, err := l.write(ctx, storage.Conditions{GenerationMatch: l.generation}, time.Time{})
	if leasePreconditionFailed(err) {
		l.generation = 0
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	l.generation = 0
	l.expiresAt = time.Time{}
	return nil
}

// ExpiresAt returns the last successfully persisted ownership deadline.
func (l *Lease) ExpiresAt() time.Time {
	return l.expiresAt
}

func (l *Lease) write(ctx context.Context, conditions storage.Conditions, expiresAt time.Time) (int64, error) {
	data, err := json.Marshal(leaseRecord{OwnerID: l.ownerID, ExpiresAt: expiresAt})
	if err != nil {
		return 0, fmt.Errorf("encode ingestion lease: %w", err)
	}

	writer := l.object.If(conditions).NewWriter(ctx)
	writer.ContentType = "application/json"
	// Leases are small; a single upload avoids allocating the resumable-upload buffer.
	writer.ChunkSize = 0
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return 0, fmt.Errorf("write ingestion lease: %w", err)
	}
	// Write can return before the upload finishes. Close confirms success and
	// makes the new generation available.
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("write ingestion lease: %w", err)
	}
	return writer.Attrs().Generation, nil
}

func leasePreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed
}
