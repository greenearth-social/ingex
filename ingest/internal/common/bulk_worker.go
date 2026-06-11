package common

import (
	"context"
	"sync"

	"github.com/elastic/go-elasticsearch/v9"
)

// BulkIndexWorker wraps a bulk ES operation for concurrent use.
// Must be launched with `go` after wg.Add(1). Logs the outcome; errors are
// not returned — callers that need error propagation should use direct calls.
func BulkIndexWorker[T any](
	wg *sync.WaitGroup,
	ctx context.Context,
	esClient *elasticsearch.Client,
	indexName string,
	batch []T,
	dryRun bool,
	logger *IngestLogger,
	fn func(context.Context, *elasticsearch.Client, string, []T, bool, *IngestLogger) error,
	action string,
) {
	defer wg.Done()
	if err := fn(ctx, esClient, indexName, batch, dryRun, logger); err != nil {
		logger.Error("Failed to %s %s: %v", action, indexName, err)
	} else if dryRun {
		logger.Debug("Dry-run: Would %s %d docs to %s", action, len(batch), indexName)
	} else {
		logger.Debug("%s %d docs to %s", action, len(batch), indexName)
	}
}
