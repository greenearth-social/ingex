package inference

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/greenearth/ingest/internal/common"
)

// PostEmbeddingInput is a single post to compute a post-tower embedding for
type PostEmbeddingInput struct {
	AtURI     string    // correlation ID
	Embedding []float32 // content embedding (all_MiniLM_L12_v2)
	AuthorDID string
}

// PostEmbeddingResult is the outcome for a single input. Embedding is nil and
// Err is set when the input's chunk failed.
type PostEmbeddingResult struct {
	AtURI     string
	Embedding []float32
	Err       error
}

// BatchEmbedder fans out post-tower inference over chunks of inputs with
// bounded concurrency. Failures are isolated per chunk so one failed chunk
// never affects the others (fail-open).
type BatchEmbedder struct {
	client         *Client
	chunkSize      int
	maxConcurrency int
	logger         *common.IngestLogger
}

// NewBatchEmbedder creates a BatchEmbedder. chunkSize must not exceed the
// inference service's GE_INFERENCE_MAX_BATCH.
func NewBatchEmbedder(client *Client, chunkSize, maxConcurrency int, logger *common.IngestLogger) *BatchEmbedder {
	if chunkSize <= 0 {
		chunkSize = 1024
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return &BatchEmbedder{
		client:         client,
		chunkSize:      chunkSize,
		maxConcurrency: maxConcurrency,
		logger:         logger,
	}
}

// ComputePostEmbeddings computes post-tower embeddings for all inputs,
// querying the inference service concurrently in chunks. Results are returned
// in input order. It never returns an error; per-input failures are recorded
// in PostEmbeddingResult.Err.
func (b *BatchEmbedder) ComputePostEmbeddings(ctx context.Context, inputs []PostEmbeddingInput) []PostEmbeddingResult {
	results := make([]PostEmbeddingResult, len(inputs))
	if len(inputs) == 0 {
		return results
	}

	var group errgroup.Group
	group.SetLimit(b.maxConcurrency)

	for start := 0; start < len(inputs); start += b.chunkSize {
		end := min(start+b.chunkSize, len(inputs))
		chunk := inputs[start:end]
		chunkResults := results[start:end]

		group.Go(func() error {
			embeddings := make([][]float32, len(chunk))
			authorDIDs := make([]string, len(chunk))
			for i, input := range chunk {
				embeddings[i] = input.Embedding
				authorDIDs[i] = input.AuthorDID
			}

			outputs, err := b.client.PostTowerPredict(ctx, embeddings, authorDIDs)
			for i, input := range chunk {
				chunkResults[i].AtURI = input.AtURI
				if err != nil {
					chunkResults[i].Err = err
				} else {
					chunkResults[i].Embedding = outputs[i]
				}
			}
			if err != nil {
				b.logger.Error("Post-tower inference failed for chunk of %d posts: %v", len(chunk), err)
			}
			// Always return nil so a failed chunk never cancels the others
			return nil
		})
	}

	_ = group.Wait() // goroutines never return errors
	return results
}
