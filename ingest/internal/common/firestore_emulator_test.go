package common

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
)

// These exercise FirestoreFollowStore against a real Firestore emulator — the
// ArrayUnion, ServerTimestamp and NotFound behaviour that a fake store cannot
// tell us anything about. Skipped unless an emulator is configured, so `go
// test ./...` stays hermetic in CI.
//
// To run against the local dev environment:
//
//	cd internal-tools/devenv && ./devctl up --name <name>
//	FIRESTORE_EMULATOR_HOST=127.0.0.1:8100 \
//	  GE_FIRESTORE_PROJECT=greenearth-471522 \
//	  go test ./internal/common/ -run Emulator -v
func emulatorStore(t *testing.T) (*FirestoreFollowStore, *firestore.Client) {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set; skipping emulator integration test")
	}
	projectID := os.Getenv("GE_FIRESTORE_PROJECT")
	if projectID == "" {
		projectID = "greenearth-471522"
	}
	client, err := NewFirestoreClient(context.Background(), projectID, os.Getenv("GE_FIRESTORE_DATABASE"))
	if err != nil {
		t.Fatalf("NewFirestoreClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewFirestoreFollowStore(client, NewLogger(false)), client
}

func TestEmulator_AppendFollowIsAdditiveAndIdempotent(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-append-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)

	// The API only ever writes entries that have actually been populated.
	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows":      []string{"did:plc:existing"},
		"follow_rkeys": []string{"rexisting"},
		"complete":     true,
		"generated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	if err := store.AppendFollow(ctx, docID, "r1", "did:plc:new1"); err != nil {
		t.Fatalf("AppendFollow: %v", err)
	}
	// Redelivery of the same event must not duplicate the record.
	if err := store.AppendFollow(ctx, docID, "r1", "did:plc:new1"); err != nil {
		t.Fatalf("AppendFollow (repeat): %v", err)
	}
	if err := store.AppendFollow(ctx, docID, "r2", "did:plc:new2"); err != nil {
		t.Fatalf("AppendFollow: %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	pending, _ := snap.Data()["pending_add_records"].([]interface{})
	if len(pending) != 2 || pending[0] != "r1:did:plc:new1" || pending[1] != "r2:did:plc:new2" {
		t.Fatalf("pending_add_records = %v, want [r1:did:plc:new1 r2:did:plc:new2]", pending)
	}
	// The API's own field must be untouched by our writes.
	follows, _ := snap.Data()["follows"].([]interface{})
	if len(follows) != 1 {
		t.Fatalf("follows = %v, want the API's single entry left alone", follows)
	}
}

func TestEmulator_AppendUnfollowRecordsTheRKeyWithoutTouchingFollows(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-unfollow-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)

	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows":      []string{"did:plc:a", "did:plc:b"},
		"follow_rkeys": []string{"ra", "rb"},
		"complete":     true,
		"generated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	for range 2 { // redelivery is idempotent
		if err := store.AppendUnfollow(ctx, docID, "rb"); err != nil {
			t.Fatalf("AppendUnfollow: %v", err)
		}
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	removes, _ := snap.Data()["pending_removes"].([]interface{})
	if len(removes) != 1 || removes[0] != "rb" {
		t.Fatalf("pending_removes = %v, want [rb]", removes)
	}
	// The API's own fields are left for it to reconcile on read.
	follows, _ := snap.Data()["follows"].([]interface{})
	if len(follows) != 2 {
		t.Fatalf("follows = %v, want both entries left alone", follows)
	}
}

// TestEmulator_ListUserDIDsProjectsUserDIDField guards against the specific
// bug found in the final whole-branch review: ListUserDIDs must return the
// user_did field (a real Bluesky DID usable against Bluesky's public API),
// not the Firestore document ID (which is that DID with its did:plc: prefix
// already stripped, and gets rejected by Bluesky with a 400 if passed as an
// actor identifier). It also verifies the fallback for a legacy document
// that lacks user_did, and that only precompute_artifacts users are listed.
func TestEmulator_ListUserDIDsProjectsUserDIDField(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	withField := fmt.Sprintf("userwithfield%d", suffix)
	legacyID := fmt.Sprintf("userlegacy%d", suffix)
	exploreID := fmt.Sprintf("userexplore%d", suffix)
	unsetID := fmt.Sprintf("userunset%d", suffix)

	refWithField := client.Collection(usersCollection).Doc(withField)
	if _, err := refWithField.Set(ctx, map[string]interface{}{
		"user_did":             fmt.Sprintf("did:plc:%s", withField),
		"precompute_artifacts": true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = refWithField.Delete(ctx) })

	// Legacy document missing user_did: ListUserDIDs must fall back to the
	// document ID rather than dropping the user entirely.
	refLegacy := client.Collection(usersCollection).Doc(legacyID)
	if _, err := refLegacy.Set(ctx, map[string]interface{}{
		"precompute_artifacts": true,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = refLegacy.Delete(ctx) })

	// Explore-only users — flag explicitly false, or never set — are not ours
	// and must not be listed.
	for id, data := range map[string]map[string]interface{}{
		exploreID: {"user_did": fmt.Sprintf("did:plc:%s", exploreID), "precompute_artifacts": false},
		unsetID:   {"user_did": fmt.Sprintf("did:plc:%s", unsetID)},
	} {
		ref := client.Collection(usersCollection).Doc(id)
		if _, err := ref.Set(ctx, data); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
		t.Cleanup(func() { _, _ = ref.Delete(ctx) })
	}

	dids, err := store.ListUserDIDs(ctx)
	if err != nil {
		t.Fatalf("ListUserDIDs: %v", err)
	}

	wantWithField := fmt.Sprintf("did:plc:%s", withField)
	foundWithField := false
	foundLegacy := false
	for _, did := range dids {
		if did == wantWithField {
			foundWithField = true
		}
		if did == legacyID {
			foundLegacy = true
		}
		if did == withField {
			t.Errorf("ListUserDIDs returned the bare Firestore document ID %q instead of the user_did field %q", withField, wantWithField)
		}
		if did == fmt.Sprintf("did:plc:%s", exploreID) || did == fmt.Sprintf("did:plc:%s", unsetID) {
			t.Errorf("ListUserDIDs returned %q, which is not flagged precompute_artifacts", did)
		}
	}
	if !foundWithField {
		t.Errorf("expected ListUserDIDs to include %q, got %v", wantWithField, dids)
	}
	if !foundLegacy {
		t.Errorf("expected ListUserDIDs to fall back to the document ID %q for a legacy document, got %v", legacyID, dids)
	}
}

func TestEmulator_MissingDocumentIsTolerated(t *testing.T) {
	store, _ := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-absent-%d", time.Now().UnixNano())

	// A user who has never loaded a feed has no entry. Using Update (not Set)
	// means we skip them rather than creating a document holding deltas but no
	// follows, which the API would have to treat as a miss anyway.
	if err := store.AppendFollow(ctx, docID, "r1", "did:plc:new"); err != nil {
		t.Fatalf("AppendFollow on a missing document: %v", err)
	}
	if err := store.AppendUnfollow(ctx, docID, "r1"); err != nil {
		t.Fatalf("AppendUnfollow on a missing document: %v", err)
	}
}
