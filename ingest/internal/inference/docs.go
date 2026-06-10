package inference

import (
	"context"

	"github.com/greenearth/ingest/internal/common"
)

const (
	// contentEmbeddingKey is the post content embedding used as post-tower input
	contentEmbeddingKey = "all_MiniLM_L12_v2"
	// postEmbeddingKey is the ES embeddings field the post-tower output is stored under
	postEmbeddingKey = "ge_post_embedding"
)

// AttachPostTowerEmbeddings computes post-tower embeddings for eligible docs
// and sets doc.Embeddings["ge_post_embedding"] in place. A doc is eligible
// when it is not a reply (ThreadParentPost is empty) and has a content
// embedding. Fail-open: docs whose inference chunk failed are left without
// the field so they still index. No-op when b is nil (disabled / dry-run).
func AttachPostTowerEmbeddings(ctx context.Context, b *BatchEmbedder, docs []common.ElasticsearchDoc) (embedded, skipped, failed int) {
	if b == nil || len(docs) == 0 {
		return 0, 0, 0
	}

	inputs := make([]PostEmbeddingInput, 0, len(docs))
	eligible := make([]int, 0, len(docs))
	for i, doc := range docs {
		contentEmbedding, ok := doc.Embeddings[contentEmbeddingKey]
		if doc.ThreadParentPost != "" || !ok {
			skipped++
			continue
		}
		inputs = append(inputs, PostEmbeddingInput{
			AtURI:     doc.AtURI,
			Embedding: contentEmbedding,
			AuthorDID: doc.AuthorDID,
		})
		eligible = append(eligible, i)
	}

	results := b.ComputePostEmbeddings(ctx, inputs)
	for j, result := range results {
		if result.Err != nil {
			failed++
			continue
		}
		docs[eligible[j]].Embeddings[postEmbeddingKey] = result.Embedding
		embedded++
	}

	b.logger.Metric("posts.post_tower.embedded.count", float64(embedded))
	b.logger.Metric("posts.post_tower.skipped.count", float64(skipped))
	b.logger.Metric("posts.post_tower.failed.count", float64(failed))

	return embedded, skipped, failed
}
