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
		})
	}
}