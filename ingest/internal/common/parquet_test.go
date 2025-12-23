package common

import (
	"testing"
)

// TestHitToExtractPost tests the conversion from Elasticsearch Hit to ExtractPost
func TestHitToExtractPost(t *testing.T) {
	tests := []struct {
		name     string
		hit      Hit
		expected ExtractPost
	}{
		{
			name: "basic post with all required fields",
			hit: Hit{
				Source: PostData{
					AtURI:     "at://did:plc:abc123/app.bsky.feed.post/xyz789",
					AuthorDID: "did:plc:abc123",
					Content:   "Hello, World!",
					CreatedAt: "2025-01-15T10:00:00Z",
					IndexedAt: "2025-01-15T10:01:00Z",
				},
			},
			expected: ExtractPost{
				DID:             "did:plc:abc123",
				AtURI:           "at://did:plc:abc123/app.bsky.feed.post/xyz789",
				RecordText:      "Hello, World!",
				RecordCreatedAt: "2025-01-15T10:00:00Z",
				InsertedAt:      "2025-01-15T10:01:00Z",
			},
		},
		{
			name: "post with reply thread",
			hit: Hit{
				Source: PostData{
					AtURI:            "at://did:plc:user1/app.bsky.feed.post/reply1",
					AuthorDID:        "did:plc:user1",
					Content:          "This is a reply",
					CreatedAt:        "2025-01-15T11:00:00Z",
					IndexedAt:        "2025-01-15T11:01:00Z",
					ThreadRootPost:   "at://did:plc:original/app.bsky.feed.post/root1",
					ThreadParentPost: "at://did:plc:parent/app.bsky.feed.post/parent1",
				},
			},
			expected: ExtractPost{
				DID:             "did:plc:user1",
				AtURI:           "at://did:plc:user1/app.bsky.feed.post/reply1",
				RecordText:      "This is a reply",
				RecordCreatedAt: "2025-01-15T11:00:00Z",
				InsertedAt:      "2025-01-15T11:01:00Z",
				ReplyRootURI:    "at://did:plc:original/app.bsky.feed.post/root1",
				ReplyParentURI:  "at://did:plc:parent/app.bsky.feed.post/parent1",
			},
		},
		{
			name: "post with quote",
			hit: Hit{
				Source: PostData{
					AtURI:     "at://did:plc:quoter/app.bsky.feed.post/quote1",
					AuthorDID: "did:plc:quoter",
					Content:   "Quoting this great post",
					CreatedAt: "2025-01-15T12:00:00Z",
					IndexedAt: "2025-01-15T12:01:00Z",
					QuotePost: "at://did:plc:original/app.bsky.feed.post/quoted1",
				},
			},
			expected: ExtractPost{
				DID:             "did:plc:quoter",
				AtURI:           "at://did:plc:quoter/app.bsky.feed.post/quote1",
				EmbedQuoteURI:   "at://did:plc:original/app.bsky.feed.post/quoted1",
				RecordText:      "Quoting this great post",
				RecordCreatedAt: "2025-01-15T12:00:00Z",
				InsertedAt:      "2025-01-15T12:01:00Z",
			},
		},
		{
			name: "post with embeddings",
			hit: Hit{
				Source: PostData{
					AtURI:     "at://did:plc:embedder/app.bsky.feed.post/embed1",
					AuthorDID: "did:plc:embedder",
					Content:   "Post with embeddings",
					CreatedAt: "2025-01-15T13:00:00Z",
					IndexedAt: "2025-01-15T13:01:00Z",
					Embeddings: map[string][]float32{
						"model1": {1.0, 2.0, 3.0},
						"model2": {0.5, 0.6, 0.7},
					},
				},
			},
			expected: ExtractPost{
				DID:             "did:plc:embedder",
				AtURI:           "at://did:plc:embedder/app.bsky.feed.post/embed1",
				RecordText:      "Post with embeddings",
				RecordCreatedAt: "2025-01-15T13:00:00Z",
				InsertedAt:      "2025-01-15T13:01:00Z",
				Embeddings: map[string]string{
					"model1": "c${NkXs~BsU~m8;2LK5}0e}",
					"model2": "c${NkQ}>9rE6?Q=u<&;10SZD",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HitToExtractPost(tt.hit)

			if result.DID != tt.expected.DID {
				t.Errorf("DID = %q, expected %q", result.DID, tt.expected.DID)
			}
			if result.AtURI != tt.expected.AtURI {
				t.Errorf("AtURI = %q, expected %q", result.AtURI, tt.expected.AtURI)
			}
			if result.RecordText != tt.expected.RecordText {
				t.Errorf("RecordText = %q, expected %q", result.RecordText, tt.expected.RecordText)
			}
			if result.RecordCreatedAt != tt.expected.RecordCreatedAt {
				t.Errorf("RecordCreatedAt = %q, expected %q", result.RecordCreatedAt, tt.expected.RecordCreatedAt)
			}
			if result.InsertedAt != tt.expected.InsertedAt {
				t.Errorf("InsertedAt = %q, expected %q", result.InsertedAt, tt.expected.InsertedAt)
			}
			if result.EmbedQuoteURI != tt.expected.EmbedQuoteURI {
				t.Errorf("EmbedQuoteURI = %q, expected %q", result.EmbedQuoteURI, tt.expected.EmbedQuoteURI)
			}
			if result.ReplyParentURI != tt.expected.ReplyParentURI {
				t.Errorf("ReplyParentURI = %q, expected %q", result.ReplyParentURI, tt.expected.ReplyParentURI)
			}
			if result.ReplyRootURI != tt.expected.ReplyRootURI {
				t.Errorf("ReplyRootURI = %q, expected %q", result.ReplyRootURI, tt.expected.ReplyRootURI)
			}

			// Check embeddings length
			if len(result.Embeddings) != len(tt.expected.Embeddings) {
				t.Errorf("Embeddings count = %d, expected %d", len(result.Embeddings), len(tt.expected.Embeddings))
			}

			// If embeddings are expected, verify keys exist (we can't compare exact values without decoding)
			for modelName := range tt.expected.Embeddings {
				if _, exists := result.Embeddings[modelName]; !exists {
					t.Errorf("Missing embedding for model %q", modelName)
				}
			}
		})
	}
}

// TestHitsToExtractPosts tests batch conversion
func TestHitsToExtractPosts(t *testing.T) {
	hits := []Hit{
		{
			Source: PostData{
				AtURI:     "at://did:plc:user1/app.bsky.feed.post/post1",
				AuthorDID: "did:plc:user1",
				Content:   "First post",
				CreatedAt: "2025-01-15T10:00:00Z",
				IndexedAt: "2025-01-15T10:01:00Z",
			},
		},
		{
			Source: PostData{
				AtURI:     "at://did:plc:user2/app.bsky.feed.post/post2",
				AuthorDID: "did:plc:user2",
				Content:   "Second post",
				CreatedAt: "2025-01-15T11:00:00Z",
				IndexedAt: "2025-01-15T11:01:00Z",
			},
		},
		{
			Source: PostData{
				AtURI:     "at://did:plc:user3/app.bsky.feed.post/post3",
				AuthorDID: "did:plc:user3",
				Content:   "Third post",
				CreatedAt: "2025-01-15T12:00:00Z",
				IndexedAt: "2025-01-15T12:01:00Z",
			},
		},
	}

	result := HitsToExtractPosts(hits)

	if len(result) != len(hits) {
		t.Fatalf("Expected %d posts, got %d", len(hits), len(result))
	}

	// Verify each post retained its unique AtURI
	expectedAtURIs := []string{
		"at://did:plc:user1/app.bsky.feed.post/post1",
		"at://did:plc:user2/app.bsky.feed.post/post2",
		"at://did:plc:user3/app.bsky.feed.post/post3",
	}

	for i, post := range result {
		if post.AtURI != expectedAtURIs[i] {
			t.Errorf("Post %d: AtURI = %q, expected %q", i, post.AtURI, expectedAtURIs[i])
		}
		if post.DID != hits[i].Source.AuthorDID {
			t.Errorf("Post %d: DID = %q, expected %q", i, post.DID, hits[i].Source.AuthorDID)
		}
	}
}

// TestExtractPostAtURIRequired verifies that AtURI is always populated
func TestExtractPostAtURIRequired(t *testing.T) {
	hit := Hit{
		Source: PostData{
			AtURI:     "at://did:plc:test/app.bsky.feed.post/test123",
			AuthorDID: "did:plc:test",
			Content:   "Test content",
			CreatedAt: "2025-01-15T10:00:00Z",
			IndexedAt: "2025-01-15T10:01:00Z",
		},
	}

	result := HitToExtractPost(hit)

	if result.AtURI == "" {
		t.Error("AtURI should not be empty")
	}

	if result.AtURI != hit.Source.AtURI {
		t.Errorf("AtURI = %q, expected %q", result.AtURI, hit.Source.AtURI)
	}
}
