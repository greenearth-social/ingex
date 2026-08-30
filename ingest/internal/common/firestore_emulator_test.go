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

func TestEmulator_AppendPendingFollowIsAdditiveAndIdempotent(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-append-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)

	// The API only ever writes entries that have actually been populated.
	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows":      []string{"did:plc:existing"},
		"complete":     true,
		"generated_at": time.Now().UTC(),
		"pending_adds": []string{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	if err := store.AppendPendingFollow(ctx, docID, "did:plc:new1"); err != nil {
		t.Fatalf("AppendPendingFollow: %v", err)
	}
	// Redelivery of the same event must not duplicate the DID.
	if err := store.AppendPendingFollow(ctx, docID, "did:plc:new1"); err != nil {
		t.Fatalf("AppendPendingFollow (repeat): %v", err)
	}
	if err := store.AppendPendingFollow(ctx, docID, "did:plc:new2"); err != nil {
		t.Fatalf("AppendPendingFollow: %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	pending, _ := snap.Data()["pending_adds"].([]interface{})
	if len(pending) != 2 {
		t.Fatalf("pending_adds = %v, want 2 distinct entries", pending)
	}
	// The API's own field must be untouched by our writes.
	follows, _ := snap.Data()["follows"].([]interface{})
	if len(follows) != 1 {
		t.Fatalf("follows = %v, want the API's single entry left alone", follows)
	}
}

func TestEmulator_InvalidateStampsWithoutTouchingFollows(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-invalidate-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)

	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows":      []string{"did:plc:a", "did:plc:b"},
		"complete":     true,
		"generated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	if err := store.InvalidateFollows(ctx, docID); err != nil {
		t.Fatalf("InvalidateFollows: %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if snap.Data()["invalidated_at"] == nil {
		t.Fatal("invalidated_at was not stamped")
	}
	follows, _ := snap.Data()["follows"].([]interface{})
	if len(follows) != 2 {
		t.Fatalf("follows = %v, want both entries still served while the refresh runs", follows)
	}
}

func TestEmulator_WriteFollowsThenReadEntry(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-write-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	if err := store.WriteFollows(ctx, docID, []string{"did:plc:a", "did:plc:b"}, true, 30); err != nil {
		t.Fatalf("WriteFollows: %v", err)
	}

	entry, err := store.ReadEntry(ctx, docID)
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if len(entry.Follows) != 2 || !entry.Complete {
		t.Errorf("unexpected entry: %+v", entry)
	}
	if entry.GeneratedAt == nil {
		t.Error("expected GeneratedAt to be set")
	}
	if len(entry.PendingAdds) != 0 {
		t.Errorf("expected pending_adds reset to empty, got %v", entry.PendingAdds)
	}
}

func TestEmulator_ReadEntryMissingDocReturnsNil(t *testing.T) {
	store, _ := emulatorStore(t)
	entry, err := store.ReadEntry(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry != nil {
		t.Errorf("expected nil entry, got %+v", entry)
	}
}

func TestEmulator_WriteFollowsResetsPendingAddsAndInvalidatedAt(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-reset-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows": []string{"did:plc:old"}, "complete": true,
		"generated_at": time.Now().UTC(), "pending_adds": []string{"did:plc:new"},
		"invalidated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.WriteFollows(ctx, docID, []string{"did:plc:old", "did:plc:new"}, true, 30); err != nil {
		t.Fatalf("WriteFollows: %v", err)
	}

	entry, err := store.ReadEntry(ctx, docID)
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.InvalidatedAt != nil {
		t.Error("expected invalidated_at cleared after a fresh walk")
	}
	if len(entry.PendingAdds) != 0 {
		t.Errorf("expected pending_adds cleared, got %v", entry.PendingAdds)
	}
}

// TestEmulator_ReadEntryWithMalformedFollowsReturnsNil covers the correction
// applied to the brief: an unreadable document (wrong-typed field) is a miss,
// not an error, mirroring api's FollowedUsersCache._read comment — "An
// unreadable document is a miss, not an error: it will be overwritten by the
// next refresh." Without this, a malformed document would get permanently
// stuck in the batch job's read_error/failed outcome instead of being
// refreshed.
func TestEmulator_ReadEntryWithMalformedFollowsReturnsNil(t *testing.T) {
	store, client := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-malformed-%d", time.Now().UnixNano())
	ref := client.Collection(followedUsersCacheCollection).Doc(docID)
	t.Cleanup(func() { _, _ = ref.Delete(ctx) })

	// `follows` should be an array; write it as a string to force a decode error.
	if _, err := ref.Set(ctx, map[string]interface{}{
		"follows":      "not-an-array",
		"complete":     true,
		"generated_at": time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	entry, err := store.ReadEntry(ctx, docID)
	if err != nil {
		t.Fatalf("ReadEntry: expected nil error for malformed document, got %v", err)
	}
	if entry != nil {
		t.Errorf("expected nil entry for malformed document, got %+v", entry)
	}
}

func TestEmulator_MissingDocumentIsTolerated(t *testing.T) {
	store, _ := emulatorStore(t)
	ctx := context.Background()
	docID := fmt.Sprintf("test-absent-%d", time.Now().UnixNano())

	// A user who has never loaded a feed has no entry. Using Update (not Set)
	// means we skip them rather than creating a document holding deltas but no
	// follows, which the API would have to treat as a miss anyway.
	if err := store.AppendPendingFollow(ctx, docID, "did:plc:new"); err != nil {
		t.Fatalf("AppendPendingFollow on a missing document: %v", err)
	}
	if err := store.InvalidateFollows(ctx, docID); err != nil {
		t.Fatalf("InvalidateFollows on a missing document: %v", err)
	}
}
