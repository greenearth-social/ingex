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

// ListUserDIDs returns the DIDs of the users the API precomputes artifacts
// for (precompute_artifacts == true, set by the API on a user's first real
// PostSeen; api#489).
//
// Not every user: the users collection also holds every account that has
// ever loaded a feed preview in Bluesky's Explore tab — well over a million
// documents, against a few thousand real users. Listing all of them exceeded
// Firestore's deadline on every attempt while still billing for each document
// read before it gave up. The equality filter is served by Firestore's
// automatic single-field index, so the read cost is one per real user.
//
// Projected to the user_did field only — the documents themselves are large
// and no other field matters here. user_did is the real Bluesky DID (with
// its did:plc: prefix intact); the Firestore document ID (doc.Ref.ID) is
// that DID with the prefix stripped (see UserDocID) and is NOT usable as an
// actor identifier against Bluesky's API, which rejects it with a 400. Falls
// back to doc.Ref.ID only for a legacy/malformed document missing user_did.
func (s *FirestoreFollowStore) ListUserDIDs(ctx context.Context) ([]string, error) {
	var dids []string
	iter := s.client.Collection(usersCollection).
		Where("precompute_artifacts", "==", true).
		Select("user_did").
		Documents(ctx)
	defer iter.Stop()
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing users: %w", err)
		}
		dids = append(dids, didFromUserDoc(doc.Data(), doc.Ref.ID))
	}
	return dids, nil
}

// didFromUserDoc extracts the user_did field from a users/ document, falling
// back to the Firestore document ID for a legacy/malformed document missing
// user_did.
func didFromUserDoc(data map[string]interface{}, docID string) string {
	did, _ := data["user_did"].(string)
	if did == "" {
		did = docID // legacy document missing user_did; best effort
	}
	return did
}

// AppendFollow records a follow userDocID made, as "rkey:subjectDID".
//
// The rkey is what lets a later unfollow — which carries nothing else — be
// matched back to subjectDID (see AppendUnfollow). ArrayUnion is atomic and
// needs no read first, so this never contends with the API rewriting `follows`
// on a refresh, and repeated delivery of the same event is idempotent.
func (s *FirestoreFollowStore) AppendFollow(ctx context.Context, userDocID, rkey, subjectDID string) error {
	_, err := s.doc(userDocID).Update(ctx, []firestore.Update{
		{Path: "pending_add_records", Value: firestore.ArrayUnion(rkey + ":" + subjectDID)},
	})
	return s.tolerateMissing(err, userDocID)
}

// AppendUnfollow records the rkey of a follow record userDocID deleted.
//
// A jetstream delete carries only did/collection/rkey. The API's cache stores
// each follow's rkey alongside its DID, so it drops the unfollowed account on
// its next read without re-walking Bluesky.
func (s *FirestoreFollowStore) AppendUnfollow(ctx context.Context, userDocID, rkey string) error {
	_, err := s.doc(userDocID).Update(ctx, []firestore.Update{
		{Path: "pending_removes", Value: firestore.ArrayUnion(rkey)},
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
