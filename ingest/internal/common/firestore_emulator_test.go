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
