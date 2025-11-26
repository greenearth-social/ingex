package common

import (
	"context"
	"testing"
	"time"
)

func TestCreateLikeTombstoneDoc(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name       string
		rawJSON    string
		subjectURI string
		wantAtURI  string
		wantAuthor string
	}{
		{
			name: "like delete with subject_uri",
			rawJSON: `{
				"did": "did:plc:author123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "delete",
					"collection": "app.bsky.feed.like",
					"rkey": "likekey456"
				}
			}`,
			subjectURI: "at://did:plc:postauthor/app.bsky.feed.post/postkey789",
			wantAtURI:  "at://did:plc:author123/app.bsky.feed.like/likekey456",
			wantAuthor: "did:plc:author123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewJetstreamMessage(tt.rawJSON, logger)

			if !msg.IsLikeDelete() {
				t.Fatal("Expected message to be a like delete")
			}

			tombstone := CreateLikeTombstoneDoc(msg, tt.subjectURI)

			if tombstone.AtURI != tt.wantAtURI {
				t.Errorf("AtURI = %v, want %v", tombstone.AtURI, tt.wantAtURI)
			}

			if tombstone.AuthorDID != tt.wantAuthor {
				t.Errorf("AuthorDID = %v, want %v", tombstone.AuthorDID, tt.wantAuthor)
			}

			if tombstone.SubjectURI != tt.subjectURI {
				t.Errorf("SubjectURI = %v, want %v", tombstone.SubjectURI, tt.subjectURI)
			}

			if tombstone.DeletedAt == "" {
				t.Error("Expected DeletedAt to be set")
			}

			deletedAt, err := time.Parse(time.RFC3339, tombstone.DeletedAt)
			if err != nil {
				t.Errorf("Expected DeletedAt to be valid RFC3339, got error: %v", err)
			}

			expectedDeletedAt := time.Unix(0, msg.GetTimeUs()*1000)
			if deletedAt.Unix() != expectedDeletedAt.Unix() {
				t.Errorf("DeletedAt = %v, want %v", deletedAt, expectedDeletedAt)
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
	}
}

func TestCreateLikeTombstoneDoc_EmptySubjectURI(t *testing.T) {
	logger := NewLogger(false)

	rawJSON := `{
		"did": "did:plc:author123",
		"time_us": 1764183883593160,
		"kind": "commit",
		"commit": {
			"operation": "delete",
			"collection": "app.bsky.feed.like",
			"rkey": "likekey456"
		}
	}`

	msg := NewJetstreamMessage(rawJSON, logger)
	tombstone := CreateLikeTombstoneDoc(msg, "")

	if tombstone.SubjectURI != "" {
		t.Errorf("Expected empty SubjectURI, got %v", tombstone.SubjectURI)
	}
}

func TestBulkIndexLikeTombstones_DryRun(t *testing.T) {
	logger := NewLogger(false)

	tombstone := LikeTombstoneDoc{
		AtURI:      "at://did:plc:test/app.bsky.feed.like/123",
		AuthorDID:  "did:plc:test",
		SubjectURI: "at://did:plc:other/app.bsky.feed.post/456",
		DeletedAt:  time.Now().UTC().Format(time.RFC3339),
		IndexedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	err := BulkIndexLikeTombstones(context.TODO(), nil, "like_tombstones", []LikeTombstoneDoc{tombstone}, true, logger)
	if err != nil {
		t.Errorf("Expected no error in dry-run mode, got: %v", err)
	}
}

func TestBulkIndexLikeTombstones_EmptyBatch(t *testing.T) {
	logger := NewLogger(false)

	err := BulkIndexLikeTombstones(context.TODO(), nil, "like_tombstones", []LikeTombstoneDoc{}, false, logger)
	if err != nil {
		t.Errorf("Expected no error for empty batch, got: %v", err)
	}
}


func TestLikeIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		atURI     string
		authorDID string
	}{
		{
			name:      "valid like identifier",
			atURI:     "at://did:plc:author/app.bsky.feed.like/key123",
			authorDID: "did:plc:author",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := LikeIdentifier{
				AtURI:     tt.atURI,
				AuthorDID: tt.authorDID,
			}

			if id.AtURI != tt.atURI {
				t.Errorf("AtURI = %v, want %v", id.AtURI, tt.atURI)
			}

			if id.AuthorDID != tt.authorDID {
				t.Errorf("AuthorDID = %v, want %v", id.AuthorDID, tt.authorDID)
			}
		})
	}
}
