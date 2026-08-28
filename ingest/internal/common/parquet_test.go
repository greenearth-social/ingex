package common

import (
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
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
			// Exports the post-tower vector and the L12 content vector that is its
			// training input. Families that are no longer ingested must not
			// reappear here from older documents (ingex#444).
			name: "post with embeddings exports only the post-tower and content vectors",
			hit: Hit{
				Source: PostData{
					AtURI:     "at://did:plc:embedder/app.bsky.feed.post/embed1",
					AuthorDID: "did:plc:embedder",
					Content:   "Post with embeddings",
					CreatedAt: "2025-01-15T13:00:00Z",
					IndexedAt: "2025-01-15T13:01:00Z",
					Embeddings: map[string][]float32{
						GEPostEmbeddingFamily:        {1.0, 2.0, 3.0},
						ContentEmbeddingFamily:       {0.5, 0.6, 0.7},
						"google_embeddinggemma_300m": {0.1, 0.2, 0.3},
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
					GEPostEmbeddingFamily:  "c${NkXs~BsU~m8;2LK5}0e}",
					ContentEmbeddingFamily: "c${NkQ}>9rE6?Q=u<&;10SZD",
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

// TestInferenceHitToExtractInference tests the conversion from InferenceHit to ExtractInference
func TestInferenceHitToExtractInference(t *testing.T) {
	tests := []struct {
		name     string
		hit      InferenceHit
		expected ExtractInference
	}{
		{
			name: "basic inference hit",
			hit: InferenceHit{
				Source: InferenceSource{
					AtURI:      "at://did:plc:abc123/app.bsky.feed.post/xyz789",
					IndexedAt:  "2025-01-15T10:01:00Z",
					Inferences: []byte(`{"sentiment":{"label":"positive","score":0.9}}`),
				},
			},
			expected: ExtractInference{
				AtURI:      "at://did:plc:abc123/app.bsky.feed.post/xyz789",
				IndexedAt:  "2025-01-15T10:01:00Z",
				Inferences: `{"sentiment":{"label":"positive","score":0.9}}`,
			},
		},
		{
			name: "inference hit with empty inferences",
			hit: InferenceHit{
				Source: InferenceSource{
					AtURI:      "at://did:plc:user1/app.bsky.feed.post/post1",
					IndexedAt:  "2025-01-15T11:00:00Z",
					Inferences: []byte(`{}`),
				},
			},
			expected: ExtractInference{
				AtURI:      "at://did:plc:user1/app.bsky.feed.post/post1",
				IndexedAt:  "2025-01-15T11:00:00Z",
				Inferences: `{}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferenceHitToExtractInference(tt.hit)

			if result.AtURI != tt.expected.AtURI {
				t.Errorf("AtURI = %q, expected %q", result.AtURI, tt.expected.AtURI)
			}
			if result.IndexedAt != tt.expected.IndexedAt {
				t.Errorf("IndexedAt = %q, expected %q", result.IndexedAt, tt.expected.IndexedAt)
			}
			if result.Inferences != tt.expected.Inferences {
				t.Errorf("Inferences = %q, expected %q", result.Inferences, tt.expected.Inferences)
			}
		})
	}
}

// TestInferenceHitsToExtractInferences tests batch conversion of inference hits
func TestInferenceHitsToExtractInferences(t *testing.T) {
	hits := []InferenceHit{
		{
			Source: InferenceSource{
				AtURI:      "at://did:plc:user1/app.bsky.feed.post/post1",
				IndexedAt:  "2025-01-15T10:00:00Z",
				Inferences: []byte(`{"sentiment":{"label":"positive","score":0.9}}`),
			},
		},
		{
			Source: InferenceSource{
				AtURI:      "at://did:plc:user2/app.bsky.feed.post/post2",
				IndexedAt:  "2025-01-15T11:00:00Z",
				Inferences: []byte(`{"toxicity":{"score":0.1}}`),
			},
		},
	}

	result := InferenceHitsToExtractInferences(hits)

	if len(result) != len(hits) {
		t.Fatalf("Expected %d inferences, got %d", len(hits), len(result))
	}

	for i, inf := range result {
		if inf.AtURI != hits[i].Source.AtURI {
			t.Errorf("Inference %d: AtURI = %q, expected %q", i, inf.AtURI, hits[i].Source.AtURI)
		}
		if inf.IndexedAt != hits[i].Source.IndexedAt {
			t.Errorf("Inference %d: IndexedAt = %q, expected %q", i, inf.IndexedAt, hits[i].Source.IndexedAt)
		}
		if inf.Inferences != string(hits[i].Source.Inferences) {
			t.Errorf("Inference %d: Inferences = %q, expected %q", i, inf.Inferences, string(hits[i].Source.Inferences))
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

// Perspective scores are exported because the API sunsets in January and these
// are the training labels for its replacement — the reason for scoring at
// ingest at all (api#368). An export that silently dropped them would leave us
// with a corpus in Elasticsearch that ages out before anyone noticed.
func TestHitToExtractPostCarriesPerspectiveScores(t *testing.T) {
	score := 0.42
	post := HitToExtractPost(Hit{
		Source: PostData{
			AtURI:                    "at://did:plc:a/app.bsky.feed.post/1",
			AuthorDID:                "did:plc:a",
			PerspectiveScores:        map[string]float64{"toxicity": 0.1, "insult": 0.2},
			CombinedPerspectiveScore: &score,
			PerspectiveScoredAt:      "2026-08-28T00:00:00Z",
		},
	})

	if post.CombinedPerspectiveScore == nil || *post.CombinedPerspectiveScore != 0.42 {
		t.Errorf("combined score = %v, want 0.42", post.CombinedPerspectiveScore)
	}
	if post.PerspectiveScores["toxicity"] != 0.1 || post.PerspectiveScores["insult"] != 0.2 {
		t.Errorf("attribute scores = %v", post.PerspectiveScores)
	}
	if post.PerspectiveScoredAt != "2026-08-28T00:00:00Z" {
		t.Errorf("scored_at = %q", post.PerspectiveScoredAt)
	}
}

// An unscored post exports as absent, not as zero. Training has to be able to
// tell "we never scored this" from "this scored 0.0", which is maximally
// toxic — the two would otherwise be the same row.
func TestHitToExtractPostOmitsAbsentPerspectiveScores(t *testing.T) {
	post := HitToExtractPost(Hit{
		Source: PostData{AtURI: "at://did:plc:a/app.bsky.feed.post/1", AuthorDID: "did:plc:a"},
	})

	if post.CombinedPerspectiveScore != nil {
		t.Errorf("combined score = %v, want nil", *post.CombinedPerspectiveScore)
	}
	if post.PerspectiveScores != nil {
		t.Errorf("attribute scores = %v, want nil", post.PerspectiveScores)
	}
	if post.PerspectiveScoredAt != "" {
		t.Errorf("scored_at = %q, want empty", post.PerspectiveScoredAt)
	}
}

// A round trip through an actual Parquet file, because the struct tags are the
// part most likely to be wrong in a way unit tests on HitToExtractPost cannot
// see: parquet-go has to be able to represent a map[string]float64 and an
// optional float64 at all, and a schema error here would only surface when the
// extract job next ran in production.
func TestExtractPostPerspectiveRoundTripsThroughParquet(t *testing.T) {
	score := 0.42
	original := []ExtractPost{
		{
			DID:                      "did:plc:a",
			AtURI:                    "at://did:plc:a/app.bsky.feed.post/1",
			RecordText:               "scored",
			PerspectiveScores:        map[string]float64{"toxicity": 0.125, "insult": 0.25},
			CombinedPerspectiveScore: &score,
			PerspectiveScoredAt:      "2026-08-28T00:00:00Z",
		},
		{
			// Attempted but unscorable: stamped, no scores.
			DID:                 "did:plc:b",
			AtURI:               "at://did:plc:b/app.bsky.feed.post/2",
			RecordText:          "日本語",
			PerspectiveScoredAt: "2026-08-28T00:00:00Z",
		},
		{
			// Never attempted: nothing at all.
			DID:        "did:plc:c",
			AtURI:      "at://did:plc:c/app.bsky.feed.post/3",
			RecordText: "unscored",
		},
	}

	path := filepath.Join(t.TempDir(), "posts.parquet")
	if err := parquet.WriteFile(path, original); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	readBack, err := parquet.ReadFile[ExtractPost](path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(readBack) != len(original) {
		t.Fatalf("read %d rows, wrote %d", len(readBack), len(original))
	}

	if got := readBack[0].PerspectiveScores; got["toxicity"] != 0.125 || got["insult"] != 0.25 {
		t.Errorf("attribute scores round-tripped as %v", got)
	}
	if readBack[0].CombinedPerspectiveScore == nil || *readBack[0].CombinedPerspectiveScore != 0.42 {
		t.Errorf("combined score round-tripped as %v", readBack[0].CombinedPerspectiveScore)
	}
	if readBack[0].PerspectiveScoredAt != "2026-08-28T00:00:00Z" {
		t.Errorf("scored_at round-tripped as %q", readBack[0].PerspectiveScoredAt)
	}

	// The unscorable row keeps its stamp and gains no score.
	if readBack[1].PerspectiveScoredAt == "" {
		t.Error("the unscorable row lost its stamp")
	}
	if readBack[1].CombinedPerspectiveScore != nil {
		t.Errorf("the unscorable row gained a score: %v", *readBack[1].CombinedPerspectiveScore)
	}

	// The never-attempted row must stay distinguishable from a genuine 0.0.
	if readBack[2].CombinedPerspectiveScore != nil {
		t.Errorf("an unscored row read back as %v, not absent", *readBack[2].CombinedPerspectiveScore)
	}
	if readBack[2].PerspectiveScoredAt != "" {
		t.Errorf("an unscored row gained a stamp: %q", readBack[2].PerspectiveScoredAt)
	}
}
