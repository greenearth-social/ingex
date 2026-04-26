// File: ingest/internal/common/elasticsearch_like_count_test.go
package common

import (
	"context"
	"testing"
)

func TestCreateElasticsearchDoc_WithLikeCount(t *testing.T) {
	logger := NewLogger(false)

	validRawPost := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "test post content",
					"createdAt": "2024-01-01T00:00:00Z"
				}
			}
		}
	}`

	// Test with zero likes
	msg := NewMegaStreamMessage("at://did:test/app.bsky.feed.post/test123", "did:test", validRawPost, "{}", logger)
	doc := CreateElasticsearchDoc(msg, 0)

	if doc.LikeCount != 0 {
		t.Errorf("Expected LikeCount = 0, got %d", doc.LikeCount)
	}

	if doc.AtURI != "at://did:test/app.bsky.feed.post/test123" {
		t.Errorf("Expected AtURI to be set correctly, got %s", doc.AtURI)
	}

	if doc.Content != "test post content" {
		t.Errorf("Expected Content to be set correctly, got %s", doc.Content)
	}

	// Test with positive likes
	doc2 := CreateElasticsearchDoc(msg, 42)
	if doc2.LikeCount != 42 {
		t.Errorf("Expected LikeCount = 42, got %d", doc2.LikeCount)
	}

	// Test with large like count
	doc3 := CreateElasticsearchDoc(msg, 10000)
	if doc3.LikeCount != 10000 {
		t.Errorf("Expected LikeCount = 10000, got %d", doc3.LikeCount)
	}
}