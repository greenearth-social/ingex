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

type leaseRecord struct {
	OwnerID   string    `json:"owner_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Lease coordinates one active ingester using GCS generation preconditions.
// Calls must be serialized; the worker must stop before ExpiresAt if renewal fails.
type Lease struct {
	object     *storage.ObjectHandle
	ownerID    string
	generation int64
	expiresAt  time.Time
	now        func() time.Time
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
	conditions := storage.Conditions{DoesNotExist: true}
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

	expiresAt := l.now().Add(ttl)
	generation, err := l.write(ctx, conditions, expiresAt)
	if leasePreconditionFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	l.generation = generation
	l.expiresAt = expiresAt
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
		l.generation = 0
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	l.generation = generation
	l.expiresAt = expiresAt
	if !l.now().Before(expiresAt) {
		return ErrLeaseLost
	}
	return nil
}

// Release makes the lease available after ingestion and cursor persistence stop.
// The generation check prevents an old owner from releasing its successor's lease.
func (l *Lease) Release(ctx context.Context) error {
	if l.generation == 0 {
		return ErrLeaseLost
	}
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
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("write ingestion lease: %w", err)
	}
	return writer.Attrs().Generation, nil
}

func leasePreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed
}
