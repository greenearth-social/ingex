package main

import (
	"strings"
	"testing"
	"time"
)

func TestDeleteMessageFlow(t *testing.T) {
	logger := NewLogger(false)

	postAtURI := "at://did:plc:test123/app.bsky.feed.post/abc123"
	postDID := "did:plc:test123"
	postContent := "Hello, world!"
	postCreatedAt := time.Now().UTC().Format(time.RFC3339)

	postRawJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "` + postContent + `",
					"createdAt": "` + postCreatedAt + `"
				}
			}
		}
	}`

	deleteRawJSON := `{
		"message": {
			"commit": {
				"operation": "delete"
			}
		}
	}`

	postMsg := NewMegaStreamMessage(postAtURI, postDID, postRawJSON, "{}", logger)
	if postMsg.IsDelete() {
		t.Error("Expected post message to not be a delete")
	}

	postDoc := CreateElasticsearchDoc(postMsg)
	if postDoc.AtURI != postAtURI {
		t.Errorf("Expected AtURI %s, got %s", postAtURI, postDoc.AtURI)
	}
	if postDoc.AuthorDID != postDID {
		t.Errorf("Expected AuthorDID %s, got %s", postDID, postDoc.AuthorDID)
	}
	if postDoc.Content != postContent {
		t.Errorf("Expected Content %s, got %s", postContent, postDoc.Content)
	}
	if postDoc.IndexedAt == "" {
		t.Error("Expected post to have IndexedAt timestamp")
	}

	_, err := time.Parse(time.RFC3339, postDoc.IndexedAt)
	if err != nil {
		t.Errorf("Expected IndexedAt to be valid RFC3339 timestamp, got error: %v", err)
	}

	deleteMsg := NewMegaStreamMessage(postAtURI, postDID, deleteRawJSON, "{}", logger)
	if !deleteMsg.IsDelete() {
		t.Error("Expected delete message to be a delete")
	}

	tombstoneDoc := CreateTombstoneDoc(deleteMsg)
	if tombstoneDoc.AtURI != postAtURI {
		t.Errorf("Expected tombstone AtURI %s, got %s", postAtURI, tombstoneDoc.AtURI)
	}
	if tombstoneDoc.AuthorDID != postDID {
		t.Errorf("Expected tombstone AuthorDID %s, got %s", postDID, tombstoneDoc.AuthorDID)
	}
	if tombstoneDoc.DeletedAt == "" {
		t.Error("Expected tombstone to have DeletedAt timestamp")
	}
	if tombstoneDoc.IndexedAt == "" {
		t.Error("Expected tombstone to have IndexedAt timestamp")
	}

	_, err = time.Parse(time.RFC3339, tombstoneDoc.DeletedAt)
	if err != nil {
		t.Errorf("Expected tombstone DeletedAt to be valid RFC3339 timestamp, got error: %v", err)
	}

	_, err = time.Parse(time.RFC3339, tombstoneDoc.IndexedAt)
	if err != nil {
		t.Errorf("Expected tombstone IndexedAt to be valid RFC3339 timestamp, got error: %v", err)
	}
}

func TestCreateTombstoneDoc(t *testing.T) {
	logger := NewLogger(false)

	atURI := "at://did:plc:abc/app.bsky.feed.post/xyz"
	did := "did:plc:abc"

	deleteRawJSON := `{
		"message": {
			"commit": {
				"operation": "delete"
			}
		}
	}`

	msg := NewMegaStreamMessage(atURI, did, deleteRawJSON, "{}", logger)

	if !msg.IsDelete() {
		t.Fatal("Expected message to be a delete")
	}

	tombstone := CreateTombstoneDoc(msg)

	if tombstone.AtURI != atURI {
		t.Errorf("Expected AtURI %s, got %s", atURI, tombstone.AtURI)
	}

	if tombstone.AuthorDID != did {
		t.Errorf("Expected AuthorDID %s, got %s", did, tombstone.AuthorDID)
	}

	if tombstone.DeletedAt == "" {
		t.Error("Expected DeletedAt to be set")
	}

	deletedAt, err := time.Parse(time.RFC3339, tombstone.DeletedAt)
	if err != nil {
		t.Errorf("Expected DeletedAt to be valid RFC3339, got error: %v", err)
	}

	if time.Since(deletedAt) > time.Second {
		t.Errorf("Expected DeletedAt to be recent, got %v", deletedAt)
	}
}

func TestTombstoneDocFields(t *testing.T) {
	logger := NewLogger(false)

	atURI := "at://did:plc:test/app.bsky.feed.post/123"
	did := "did:plc:test"

	deleteJSON := `{"message":{"commit":{"operation":"delete"}}}`
	msg := NewMegaStreamMessage(atURI, did, deleteJSON, "{}", logger)

	tombstone := CreateTombstoneDoc(msg)

	if !strings.HasPrefix(atURI, "at://") {
		t.Error("Test data should have valid at_uri format")
	}

	if tombstone.AtURI != msg.GetAtURI() {
		t.Error("Tombstone AtURI should match message AtURI")
	}

	if tombstone.AuthorDID != msg.GetAuthorDID() {
		t.Error("Tombstone AuthorDID should match message AuthorDID")
	}

	if tombstone.DeletedAt == "" {
		t.Error("Tombstone must have DeletedAt timestamp")
	}
}

func TestDeleteMessage_IsDelete(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name       string
		rawJSON    string
		wantDelete bool
	}{
		{
			name: "delete operation",
			rawJSON: `{
				"message": {
					"commit": {
						"operation": "delete"
					}
				}
			}`,
			wantDelete: true,
		},
		{
			name: "create operation",
			rawJSON: `{
				"message": {
					"commit": {
						"operation": "create",
						"record": {
							"text": "test post"
						}
					}
				}
			}`,
			wantDelete: false,
		},
		{
			name:       "empty json",
			rawJSON:    `{}`,
			wantDelete: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMegaStreamMessage("at://test", "did:test", tt.rawJSON, "{}", logger)
			if msg.IsDelete() != tt.wantDelete {
				t.Errorf("IsDelete() = %v, want %v", msg.IsDelete(), tt.wantDelete)
			}
		})
	}
}

func TestBulkOperations_DryRun(t *testing.T) {
	logger := NewLogger(false)

	t.Run("bulkIndexTombstones dry-run returns no error", func(t *testing.T) {
		tombstone := TombstoneDoc{
			AtURI:     "at://did:plc:test/app.bsky.feed.post/123",
			AuthorDID: "did:plc:test",
			DeletedAt: time.Now().UTC().Format(time.RFC3339),
		}

		err := bulkIndexTombstones(nil, nil, "post_tombstones", []TombstoneDoc{tombstone}, true, logger)
		if err != nil {
			t.Errorf("Expected no error in dry-run mode, got: %v", err)
		}
	})

	t.Run("bulkDelete dry-run returns no error", func(t *testing.T) {
		err := bulkDelete(nil, nil, "posts", []string{"at://did:plc:test/app.bsky.feed.post/123"}, true, logger)
		if err != nil {
			t.Errorf("Expected no error in dry-run mode, got: %v", err)
		}
	})
}

func TestBulkOperations_EmptyBatch(t *testing.T) {
	logger := NewLogger(false)

	t.Run("bulkIndexTombstones empty batch returns no error", func(t *testing.T) {
		err := bulkIndexTombstones(nil, nil, "post_tombstones", []TombstoneDoc{}, false, logger)
		if err != nil {
			t.Errorf("Expected no error for empty batch, got: %v", err)
		}
	})

	t.Run("bulkDelete empty batch returns no error", func(t *testing.T) {
		err := bulkDelete(nil, nil, "posts", []string{}, false, logger)
		if err != nil {
			t.Errorf("Expected no error for empty batch, got: %v", err)
		}
	})
}

func TestTombstoneDoc_TimeUs(t *testing.T) {
	logger := NewLogger(false)

	t.Run("with time_us present", func(t *testing.T) {
		timeUs := int64(1757450801618621)
		deleteJSON := `{
			"message": {
				"time_us": 1757450801618621,
				"commit": {
					"operation": "delete"
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:test", deleteJSON, "{}", logger)
		if msg.GetTimeUs() != timeUs {
			t.Errorf("Expected GetTimeUs() = %d, got %d", timeUs, msg.GetTimeUs())
		}

		tombstone := CreateTombstoneDoc(msg)

		expectedDeletedAt := time.Unix(0, timeUs*1000).Format(time.RFC3339)
		if tombstone.DeletedAt != expectedDeletedAt {
			t.Errorf("Expected DeletedAt = %s, got %s", expectedDeletedAt, tombstone.DeletedAt)
		}

		if tombstone.IndexedAt == "" {
			t.Error("Expected IndexedAt to be set")
		}

		indexedAt, err := time.Parse(time.RFC3339, tombstone.IndexedAt)
		if err != nil {
			t.Errorf("Expected IndexedAt to be valid RFC3339, got error: %v", err)
		}

		if time.Since(indexedAt) > time.Second {
			t.Errorf("Expected IndexedAt to be recent, got %v", indexedAt)
		}
	})

	t.Run("without time_us fallback to current time", func(t *testing.T) {
		deleteJSON := `{
			"message": {
				"commit": {
					"operation": "delete"
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:test", deleteJSON, "{}", logger)
		if msg.GetTimeUs() != 0 {
			t.Errorf("Expected GetTimeUs() = 0, got %d", msg.GetTimeUs())
		}

		tombstone := CreateTombstoneDoc(msg)

		deletedAt, err := time.Parse(time.RFC3339, tombstone.DeletedAt)
		if err != nil {
			t.Errorf("Expected DeletedAt to be valid RFC3339, got error: %v", err)
		}

		if time.Since(deletedAt) > time.Second {
			t.Errorf("Expected DeletedAt to be recent (fallback to current time), got %v", deletedAt)
		}

		indexedAt, err := time.Parse(time.RFC3339, tombstone.IndexedAt)
		if err != nil {
			t.Errorf("Expected IndexedAt to be valid RFC3339, got error: %v", err)
		}

		if time.Since(indexedAt) > time.Second {
			t.Errorf("Expected IndexedAt to be recent, got %v", indexedAt)
		}
	})
}
