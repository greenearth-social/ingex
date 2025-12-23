# Megastream Ingest

This command processes BlueSky content from Megastream SQLite databases and indexes them in Elasticsearch.

## Overview

The `megastream_ingest` command:

- Reads JSON-formatted, hydrated BlueSky posts with sentence embeddings from Megastream SQLite databases
- Supports both local filesystem and S3 as data sources
- Handles both one-time ingestion and continuous monitoring (spool mode)
- Batches posts and indexes them to Elasticsearch
- Tracks processed files to avoid duplicates
- Provides graceful shutdown handling

## Configuration

Configuration is done through environment variables and command line flags.

### Command Line Flags

- `--source` - Source of SQLite files: `local` or `s3` (default: `local`)
- `--mode` - Ingestion mode: `once` (single run) or `spool` (continuous polling) (default: `once`)
- `--dry-run` - Run without writing to Elasticsearch (for testing)
- `--skip-tls-verify` - Skip TLS certificate verification (local development only)
- `--no-rewind` - Do not rewind to the last processed timestamp on startup (drops intervening data)

### Environment Variables

**Required:**

- `GE_ELASTICSEARCH_URL` - Elasticsearch cluster endpoint
- `GE_ELASTICSEARCH_API_KEY` - Elasticsearch API key with permissions:

  ```json
  {
    "indices": [
      {
        "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1"],
        "privileges": ["create_doc", "create", "delete", "index", "write", "all"]
      }
    ]
  }
  ```

**For Local Source (`--source local`):**

- `GE_LOCAL_SQLITE_DB_PATH` - Directory containing `.db.zip` files to process

**For S3 Source (`--source s3`):**

- `GE_AWS_S3_BUCKET` - S3 bucket name containing SQLite files
- `GE_AWS_S3_PREFIX` - S3 key prefix (folder path)
- `GE_AWS_REGION` - AWS region (default: `us-east-1`)

**Optional:**

- `GE_LOGGING_ENABLED` - Enable/disable logging (default: `true`)
- `GE_SPOOL_INTERVAL_SEC` - Polling interval in seconds for spool mode (default: `60`)
- `GE_MEGASTREAM_STATE_FILE` - Path to state file for cursor tracking (default: `.megastream_state.json`)

## Usage

### Basic Usage

```bash
# Process local SQLite files once
./megastream_ingest --source local --mode once

# Continuously monitor and process new local files
./megastream_ingest --source local --mode spool

# Process files from S3 once
./megastream_ingest --source s3 --mode once

# Dry-run mode (no writes to Elasticsearch)
./megastream_ingest --source local --mode once --dry-run

# Skip TLS verification (local development only)
./megastream_ingest --source local --mode once --skip-tls-verify

# Start from current time, ignoring any saved cursor
./megastream_ingest --source local --mode spool --no-rewind
```

## Elasticsearch Indexes

The command indexes data to two indexes:

### Posts Index

Posts are indexed to the `posts` index with the following structure:

```json
{
  "at_uri": "at://did:plc:xxxxx/app.bsky.feed.post/xxxxx",
  "author_did": "did:plc:xxxxx",
  "content": "Post text content",
  "created_at": "2025-10-30T12:34:56.789Z",
  "thread_root_post": "at://did:plc:yyyyy/app.bsky.feed.post/zzzzz",
  "thread_parent_post": "at://did:plc:yyyyy/app.bsky.feed.post/zzzzz",
  "quote_post": "at://did:plc:yyyyy/app.bsky.feed.post/zzzzz",
  "embeddings": {
    "all_MiniLM_L12_v2": [0.123, 0.456, ...],
    "all_MiniLM_L6_v2": [0.789, 0.012, ...]
  },
  "indexed_at": "2025-10-30T12:34:57.123Z"
}
```

### Post Tombstones Index

Deleted posts are indexed to the `post_tombstones` index:

```json
{
  "at_uri": "at://did:plc:xxxxx/app.bsky.feed.post/xxxxx",
  "author_did": "did:plc:xxxxx",
  "deleted_at": "2025-10-30T12:34:56.789Z",
  "indexed_at": "2025-10-30T12:34:57.123Z"
}
```

## Features

### Batch Processing

Posts are batched and indexed in groups of 100 to optimize Elasticsearch performance.

### Cursor-Based Resumption

The service maintains a state file (`.megastream_state.json`) that tracks the last processed timestamp. On startup:

- **With rewind enabled (default)**: Processes files from the last saved timestamp onward, preventing data loss during restarts
- **With `--no-rewind`**: Processes only files timestamped from "now" onward, skipping any intervening data
- **No cursor saved**: Processes only files timestamped from "now" onward

Files are named in the format `mega_jetstream_YYYYMMDD_hhmmss.db.zip`, and the timestamp is extracted from the filename to determine which files to process.

### Delete Handling

When a delete operation is detected:

1. A tombstone document is created in the `post_tombstones` index
2. The original post is deleted from the `posts` index

### Graceful Shutdown

The service responds to SIGINT and SIGTERM signals, completing the current batch before shutting down.

## Examples

### Local Development

```bash
# Set up environment
export GE_LOCAL_SQLITE_DB_PATH="./test_data"
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key"
export GE_LOGGING_ENABLED="true"

# Run against local ES cluster
./megastream_ingest --source local --mode once --skip-tls-verify
```

### S3 Source

```bash
# Set up environment
export GE_AWS_S3_BUCKET="my-bucket"
export GE_AWS_S3_PREFIX="megastream/databases/"
export GE_AWS_REGION="us-west-2"
export GE_ELASTICSEARCH_URL="https://my-cluster.es.amazonaws.com:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key"

# Run once
./megastream_ingest --source s3 --mode once
```

### Continuous Monitoring

```bash
# Monitor local directory every 5 minutes
export GE_SPOOL_INTERVAL_SEC="300"
export GE_LOCAL_SQLITE_DB_PATH="/data/megastream"
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key"

./megastream_ingest --source local --mode spool
```

## Building

```bash
# From the ingest directory
go build -o megastream_ingest cmd/megastream_ingest/main.go

# Or run directly
go run cmd/megastream_ingest/main.go --source local --mode once
```

## Testing

```bash
# Unit tests for common libraries
go test ./internal/common -v

# Integration test with sample data (dry-run)
export GE_LOCAL_SQLITE_DB_PATH="./test_data"
go run cmd/megastream_ingest/main.go --source local --mode once --dry-run
```

## Data Source

Megastream SQLite databases contain hydrated BlueSky posts with:

- Full post content and metadata
- Thread relationships (root, parent)
- Quote post references
- Pre-computed sentence embeddings (MiniLM-L6-v2 and MiniLM-L12-v2)
- Deletion markers

The SQLite files are expected to be in `.db.zip` format and contain a `posts` table with the following columns:

- `at_uri` - AT Protocol URI for the post
- `did` - Decentralized Identifier of the author
- `raw_post` - JSON blob with post data
- `inferences` - JSON blob with embeddings and other computed data
