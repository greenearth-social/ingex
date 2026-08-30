package common

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Firestore is the API's datastore, not ours — ingest owns Elasticsearch. The
// one thing we write here is follow deltas for the API's per-user
// followed-users cache (api#83), because jetstream is the only place on the
// system that sees follow and unfollow events as they happen.

const (
	usersCollection              = "users"
	followedUsersCacheCollection = "followed_users_cache"

	// Prefix stripped from a DID to form a Firestore document ID. Mirrors
	// app.lib.firestore.user_doc_id in the API: colons in a document ID break
	// subcollection navigation in the Firestore emulator UI. Keep the two in
	// step or we update documents nobody reads.
	userDIDPrefix = "did:plc:"
)

// UserDocID maps a DID to its Firestore user-document ID.
func UserDocID(did string) string {
	return strings.TrimPrefix(did, userDIDPrefix)
}

// NewFirestoreClient connects to Firestore, honouring the emulator when set.
//
// The Google SDK reads FIRESTORE_EMULATOR_HOST natively; Config copies the
// GE-prefixed variable into that standard name, exactly as the API does.
func NewFirestoreClient(ctx context.Context, projectID, databaseID string) (*firestore.Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("GE_FIRESTORE_PROJECT is required to write follow deltas")
	}
	if databaseID == "" || databaseID == "(default)" {
		return firestore.NewClient(ctx, projectID)
	}
	return firestore.NewClientWithDatabase(ctx, projectID, databaseID)
}

// FirestoreFollowStore applies follow deltas to the followed-users cache.
type FirestoreFollowStore struct {
	client *firestore.Client
	logger *IngestLogger
}

// NewFirestoreFollowStore returns a store backed by the given Firestore client.
func NewFirestoreFollowStore(client *firestore.Client, logger *IngestLogger) *FirestoreFollowStore {
	return &FirestoreFollowStore{client: client, logger: logger}
}

// ListUserDIDs returns the DIDs of every user the API serves.
//
// Projected to document IDs only — the documents themselves are large and
// none of their fields matter here.
func (s *FirestoreFollowStore) ListUserDIDs(ctx context.Context) ([]string, error) {
	var dids []string
	iter := s.client.Collection(usersCollection).Select().Documents(ctx)
	defer iter.Stop()
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing users: %w", err)
		}
		dids = append(dids, doc.Ref.ID)
	}
	return dids, nil
}

// AppendPendingFollow records a newly-followed DID for userDocID.
//
// ArrayUnion is atomic and needs no read first, so this never contends with
// the API rewriting `follows` on a refresh, and repeated delivery of the same
// event is idempotent.
func (s *FirestoreFollowStore) AppendPendingFollow(ctx context.Context, userDocID, subjectDID string) error {
	_, err := s.doc(userDocID).Update(ctx, []firestore.Update{
		{Path: "pending_adds", Value: firestore.ArrayUnion(subjectDID)},
	})
	return s.tolerateMissing(err, userDocID)
}

// InvalidateFollows marks userDocID's cached follows as needing a refresh.
//
// A jetstream delete carries only did/collection/rkey — there is no subject to
// remove — so the entry is invalidated and the API reconciles it against
// Bluesky on the next request.
func (s *FirestoreFollowStore) InvalidateFollows(ctx context.Context, userDocID string) error {
	_, err := s.doc(userDocID).Update(ctx, []firestore.Update{
		{Path: "invalidated_at", Value: firestore.ServerTimestamp},
	})
	return s.tolerateMissing(err, userDocID)
}

// CacheEntry is the subset of the API's FollowedUsersCacheDocument the
// backfill job needs to decide whether a user's follows need re-walking.
// Lease fields (refresh_started_at/refresh_failed_at) are omitted: the job
// runs as one serial process per invocation, not many racing API instances,
// so it has nothing to lease against.
type CacheEntry struct {
	Follows       []string
	Complete      bool
	GeneratedAt   *time.Time
	PendingAdds   []string
	InvalidatedAt *time.Time
}

// ReadEntry returns userDocID's cached follows, or nil if no walk has ever
// populated the document (matches api's FollowedUsersCache._read: a document
// holding only a lease or a delta has no generated_at and is not a real entry).
func (s *FirestoreFollowStore) ReadEntry(ctx context.Context, userDocID string) (*CacheEntry, error) {
	snap, err := s.doc(userDocID).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("reading followed-users cache for %s: %w", userDocID, err)
	}
	var raw struct {
		Follows       []string   `firestore:"follows"`
		Complete      bool       `firestore:"complete"`
		GeneratedAt   *time.Time `firestore:"generated_at"`
		PendingAdds   []string   `firestore:"pending_adds"`
		InvalidatedAt *time.Time `firestore:"invalidated_at"`
	}
	// An unreadable document is a miss, not an error: it will be overwritten
	// by the next walk. Mirrors api's FollowedUsersCache._read, which treats a
	// decode failure the same way rather than surfacing it up as an error that
	// would otherwise get the caller permanently stuck on this document.
	if err := snap.DataTo(&raw); err != nil {
		s.logger.Error("Invalid followed-users cache document for %s: %v", userDocID, err)
		return nil, nil
	}
	if raw.GeneratedAt == nil {
		return nil, nil
	}
	return &CacheEntry{
		Follows: raw.Follows, Complete: raw.Complete, GeneratedAt: raw.GeneratedAt,
		PendingAdds: raw.PendingAdds, InvalidatedAt: raw.InvalidatedAt,
	}, nil
}

// WriteFollows persists a completed (or deliberately-partial) Bluesky walk.
// Pending deltas and any invalidation are cleared: a fresh walk supersedes
// them by construction. Unlike AppendPendingFollow/InvalidateFollows this is
// a full Set, safe because only this job and api's now-removed refresh path
// ever touch `follows`/`complete`/`generated_at` — jetstream never does.
func (s *FirestoreFollowStore) WriteFollows(ctx context.Context, userDocID string, follows []string, complete bool, retentionDays int) error {
	now := time.Now().UTC()
	_, err := s.doc(userDocID).Set(ctx, map[string]interface{}{
		"follows":        follows,
		"complete":       complete,
		"generated_at":   firestore.ServerTimestamp,
		"pending_adds":   []string{},
		"invalidated_at": nil,
		"expires_at":     now.AddDate(0, 0, retentionDays),
	})
	if err != nil {
		return fmt.Errorf("writing followed-users cache for %s: %w", userDocID, err)
	}
	return nil
}

func (s *FirestoreFollowStore) doc(userDocID string) *firestore.DocumentRef {
	return s.client.Collection(followedUsersCacheCollection).Doc(userDocID)
}

// tolerateMissing swallows NotFound: the user has no cache entry yet (they
// have not loaded a feed since the cache was introduced). Update rather than
// Set is deliberate — creating a document here would leave one holding deltas
// but no follows, which the API has to treat as a miss anyway.
func (s *FirestoreFollowStore) tolerateMissing(err error, userDocID string) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		s.logger.Debug("No followed-users cache entry for %s yet; dropping delta", userDocID)
		return nil
	}
	return err
}
