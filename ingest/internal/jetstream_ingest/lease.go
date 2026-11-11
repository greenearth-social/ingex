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
// Generation preconditions decide ownership. ownerID must be unique to the process:
// it lets an instance recognize a record its own write stored after the response
// was lost, and clear it instead of waiting out the ttl.
type Lease struct {
	object     *storage.ObjectHandle
	ownerID    string
	generation int64
	expiresAt  time.Time
	now        func() time.Time
}

// NewLease uses one GCS object to coordinate ownership among instances. Each
// process needs its own ownerID; sharing one would let instances take over each
// other's unexpired leases.
func NewLease(client *storage.Client, bucket, object, ownerID string) *Lease {
	return &Lease{
		object:  client.Bucket(bucket).Object(object),
		ownerID: ownerID,
		now:     time.Now,
	}
}

// TryAcquire returns false when another instance owns the lease or wins the race
// to acquire it. It never changes an unexpired lease held by another instance.
// Callers must not be ingesting: an unexpired record with this instance's ownerID
// is taken over, since only a write whose response was lost can leave one behind.
func (l *Lease) TryAcquire(ctx context.Context, ttl time.Duration) (bool, error) {
	// Creation and takeover both use a conditional write so only one contender wins.
	conditions := storage.Conditions{DoesNotExist: true}
	record, generation, err := l.read(ctx)
	if err != nil {
		return false, err
	}
	if generation != 0 {
		if record.OwnerID != l.ownerID && l.now().Before(record.ExpiresAt) {
			return false, nil
		}
		conditions = storage.Conditions{GenerationMatch: generation}
	}

	// Count upload time against the lease lifetime rather than extending local
	// permission beyond the expiration written to GCS.
	expiresAt := l.now().Add(ttl)
	generation, err = l.write(ctx, conditions, expiresAt)
	if leasePreconditionFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Remember ownership only after GCS confirms the write.
	l.generation = generation
	l.expiresAt = expiresAt
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
		l.generation = 0
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	// Keep the previous deadline unless GCS confirms the renewal write.
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
	err := ErrLeaseLost
	if l.generation != 0 {
		// Expiring the record allows immediate takeover without deleting the object.
		_, err = l.write(ctx, storage.Conditions{GenerationMatch: l.generation}, time.Time{})
	}
	if l.generation == 0 || leasePreconditionFailed(err) {
		// The generation moved, or a renewal already found it moved. A renewal
		// that stopped ingestion may have stored this instance's record anyway;
		// expiring it spares other instances a wait for the full ttl.
		err = l.releaseOwnRecord(ctx)
	}
	if errors.Is(err, ErrLeaseLost) {
		l.generation = 0
	}
	if err != nil {
		return err
	}
	l.generation = 0
	l.expiresAt = time.Time{}
	return nil
}

// releaseOwnRecord expires the stored record only if this instance wrote it
// and it has not expired. Any other record belongs to a successor.
func (l *Lease) releaseOwnRecord(ctx context.Context) error {
	record, generation, err := l.read(ctx)
	if err != nil {
		return err
	}
	if generation == 0 || record.OwnerID != l.ownerID || !l.now().Before(record.ExpiresAt) {
		return ErrLeaseLost
	}
	_, err = l.write(ctx, storage.Conditions{GenerationMatch: generation}, time.Time{})
	if leasePreconditionFailed(err) {
		return ErrLeaseLost
	}
	return err
}

// ExpiresAt returns the last successfully persisted ownership deadline.
func (l *Lease) ExpiresAt() time.Time {
	return l.expiresAt
}

// read returns the stored record and its generation, or generation zero when
// no record exists. NewReader supplies both from the same response.
func (l *Lease) read(ctx context.Context) (leaseRecord, int64, error) {
	var record leaseRecord
	reader, err := l.object.NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return record, 0, nil
	}
	if err != nil {
		return record, 0, fmt.Errorf("read ingestion lease: %w", err)
	}
	decodeErr := json.NewDecoder(reader).Decode(&record)
	generation := reader.Attrs.Generation
	_ = reader.Close()
	if decodeErr != nil {
		return record, 0, fmt.Errorf("decode ingestion lease: %w", decodeErr)
	}
	return record, generation, nil
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
