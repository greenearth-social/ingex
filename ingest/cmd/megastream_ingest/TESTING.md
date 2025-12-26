# Testing megastream_ingest

## Unit Tests

Unit tests run without external dependencies and are executed automatically in CI:

```bash
cd ingex/ingest
go test ./cmd/megastream_ingest -v
```

## Integration Tests

Integration tests require a running Elasticsearch instance and test data files. They are automatically skipped if Elasticsearch is not available.

### Prerequisites

1. **Running Elasticsearch cluster** (see [../../index/README.md](../../index/README.md) for local setup)
2. **Test data files** in `test_data/megastream/` (see [../../test_data/README.md](../../test_data/README.md))
3. **Port-forward to Elasticsearch** (if running locally):

   ```bash
   kubectl port-forward service/greenearth-es-http 9200 -n greenearth-local
   ```

### Running Integration Tests

Set the required environment variables and run the test:

```bash
# Set Elasticsearch connection details
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key-here"

# Run integration tests
cd ingex/ingest
go test ./cmd/megastream_ingest -v -run Integration
```

### What the Integration Test Does

The `TestMegastreamIngestIntegration` test:

1. **Checks Elasticsearch availability** - Automatically skips if ES is not reachable
2. **Processes test data** - Uses the actual megastream_ingest Spooler to process files from `test_data/megastream/`
3. **Indexes documents** - Writes posts and tombstones to Elasticsearch using the real indexing code
4. **Verifies results** - Searches Elasticsearch to confirm all expected documents were indexed correctly

### Test Behavior

- **Automatically skipped if:**
  - `GE_ELASTICSEARCH_URL` or `GE_ELASTICSEARCH_API_KEY` are not set
  - Elasticsearch is not reachable
  - No test data files exist in `test_data/megastream/`
  - No documents are processed (e.g., all already indexed)

- **Test data location:** `../../test_data/megastream/`
- **State file:** Uses a temporary file (doesn't interfere with development state)
- **Indexing mode:** Creates real documents in Elasticsearch (not dry-run)

### Running in CI

Integration tests are designed to be skipped in CI environments where Elasticsearch is not available. The tests will only run when the necessary environment variables are set and Elasticsearch is accessible.

To run integration tests in CI:

1. Set up Elasticsearch in the CI environment
2. Export `GE_ELASTICSEARCH_URL` and `GE_ELASTICSEARCH_API_KEY`
3. Ensure test data files are available
4. Run: `go test ./cmd/megastream_ingest -v`

### Cleanup

The integration test creates real documents in Elasticsearch. To clean up after testing:

```bash
# Delete all posts and tombstones (use with caution!)
curl -k -X POST "https://localhost:9200/posts/_delete_by_query" \
  -H "Authorization: ApiKey $GE_ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'

curl -k -X POST "https://localhost:9200/post_tombstones/_delete_by_query" \
  -H "Authorization: ApiKey $GE_ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'
```

Or use the helper script:

```bash
../../scripts/k8s_delete_es_data_via_api.sh
```

### Simple Connection Test

To quickly verify Elasticsearch connectivity without processing data:

```bash
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key-here"

go test ./cmd/megastream_ingest -v -run TestElasticsearchConnection
```

This lightweight test only checks the connection to Elasticsearch and reports the cluster version.
