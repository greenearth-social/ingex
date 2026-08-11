package common

import (
	"context"
	"fmt"
	"strings"

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
