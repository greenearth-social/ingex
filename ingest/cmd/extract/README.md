# Extract - Elasticsearch Export Utility

Export data from Elasticsearch to Parquet files for analysis and archival.

## Usage

```bash
./extract [flags]
```

## Flags

- `--dry-run`: Preview export without writing files (default: false)
- `--skip-tls-verify`: Skip TLS verification (local development only, default: false)
- `--output-path PATH`: Override output directory (default: from PARQUET_DESTINATION)
- `--window-size-min MINUTES`: Time window in minutes from now (e.g., 240 for 4-hour lookback). Overrides start-time and end-time if set.
- `--start-time TIME`: Start time for export window in RFC3339 format (e.g., 2025-01-01T00:00:00Z)
- `--end-time TIME`: End time for export window in RFC3339 format (e.g., 2025-12-31T23:59:59Z)

## Environment Variables

- `ELASTICSEARCH_URL`: ES cluster URL (required)
- `ELASTICSEARCH_API_KEY`: ES API key (optional, recommended for production)
- `ELASTICSEARCH_TLS_SKIP_VERIFY`: Skip TLS verification (default: false)
- `PARQUET_DESTINATION`: Output destination - supports local paths (./output) or GCS paths (gs://bucket/path)
- `PARQUET_MAX_RECORDS`: Default max records per file (default: 100000)
- `EXTRACT_FETCH_SIZE`: Default fetch size (default: 1000)
- `EXTRACT_INDICES`: Comma-separated list of indices to export (default: "posts")
- `LOGGING_ENABLED`: Enable logging (default: true)

## Examples

### Export full posts index to local directory

```bash
export ELASTICSEARCH_URL="https://es.example.com:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
./extract --output-path ./exports
```

### Export to Google Cloud Storage

```bash
export ELASTICSEARCH_URL="https://es.example.com:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
export PARQUET_DESTINATION="gs://my-bucket/exports/"
./extract
```

### Export with rolling time window (last 4 hours)

```bash
./extract --window-size-min 240
```

### Export multiple indices with rolling time window

```bash
EXTRACT_INDICES="posts,likes" ./extract --window-size-min 240
```

### Export with fixed time window

```bash
./extract --start-time "2025-01-01T00:00:00Z" --end-time "2025-01-31T23:59:59Z"
```

### Dry-run to preview

```bash
./extract --dry-run --window-size-min 60
```

### Local development with self-signed certs

```bash
./extract --skip-tls-verify --output-path ./test_output
```

### Export from different indices with time range

```bash
EXTRACT_INDICES="posts_v2,likes_v2" ./extract --output-path ./v2_exports --start-time "2025-10-01T00:00:00Z"
```

### Export only posts after a specific date

```bash
./extract --start-time "2025-12-01T00:00:00Z"
```

### Export only posts before a specific date

```bash
./extract --end-time "2025-11-30T23:59:59Z"
```

## Output Format

The command exports data to Parquet files with timestamp-based naming:
- `bsky_posts_20251012_090556.parquet`
- `bsky_posts_20251012_120823.parquet`
- `bsky_likes_20251012_150430.parquet` (for likes index)
- etc.

The timestamp in the filename reflects the `record_created_at` of the most recent post in the file (posts are sorted chronologically).

Each file contains up to `max-records` posts (or all remaining posts if `max-records` is 0).

### Parquet Schema

Each record in the Parquet file contains:
- `did`: Author DID (BlueSky user identifier)
- `embed_quote_uri`: Quoted post URI (if quote post)
- `inserted_at`: Timestamp when indexed in Elasticsearch
- `record_created_at`: Post creation timestamp
- `record_text`: Post content/text
- `reply_parent_uri`: Parent post URI (if in thread)
- `reply_root_uri`: Root post URI (if in thread)

## Features

- **Pagination**: Uses Elasticsearch search_after for efficient pagination
- **Graceful shutdown**: Handles SIGTERM/SIGINT to write remaining records
- **Configurable batch sizes**: Separate control of fetch size and file size
- **Dry-run mode**: Preview export without writing files
- **Progress logging**: Real-time progress updates

## Building

```bash
cd /Users/max/Projects/greenearth/ingex/ingest
go build -o bin/extract ./cmd/extract
```

## Testing

Run in dry-run mode to test without writing files:

```bash
./bin/extract --dry-run --fetch-size 10
```
