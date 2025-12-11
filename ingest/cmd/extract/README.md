# Extract - Elasticsearch Export Utility

Export data from Elasticsearch to Parquet files for analysis and archival.

## Usage

```bash
./extract [flags]
```

## Flags

- `--dry-run`: Preview export without writing files (default: false)
- `--skip-tls-verify`: Skip TLS verification (local development only, default: false)
- `--output-path PATH`: Override output directory (default: from PARQUET_OUTPUT_PATH)
- `--index NAME`: Elasticsearch index to export (default: "posts")
- `--max-records N`: Max records per file, 0 for unlimited (default: from PARQUET_MAX_FILE_SIZE or 100000)
- `--fetch-size N`: Batch size for ES queries (default: 1000)

## Environment Variables

- `ELASTICSEARCH_URL`: ES cluster URL (required)
- `ELASTICSEARCH_API_KEY`: ES API key (optional, recommended for production)
- `ELASTICSEARCH_TLS_SKIP_VERIFY`: Skip TLS verification (default: false)
- `PARQUET_OUTPUT_PATH`: Default output directory (default: "./output")
- `PARQUET_MAX_FILE_SIZE`: Default max records per file (default: 100000)
- `EXTRACT_FETCH_SIZE`: Default fetch size (default: 1000)
- `EXTRACT_INDEX_NAME`: Default index name (default: "posts")
- `LOGGING_ENABLED`: Enable logging (default: true)

## Examples

### Export full posts index

```bash
export ELASTICSEARCH_URL="https://es.example.com:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
./extract --output-path ./exports
```

### Export with file size limit

```bash
./extract --max-records 100000 --fetch-size 5000
```

### Dry-run to preview

```bash
./extract --dry-run --index posts
```

### Local development with self-signed certs

```bash
./extract --skip-tls-verify --output-path ./test_output
```

### Export from different index

```bash
./extract --index posts_v2 --output-path ./v2_exports
```

## Output Format

The command exports data to Parquet files with the following naming convention:
- `posts_export_1.parquet`
- `posts_export_2.parquet`
- etc.

Each file contains up to `max-records` posts (or all remaining posts if `max-records` is 0).

### Parquet Schema

Each record in the Parquet file contains:
- `es_id`: Elasticsearch document ID
- `at_uri`: Post AT-URI
- `author_did`: Author DID
- `content`: Post content/text
- `created_at`: Post creation timestamp
- `thread_root_post`: Root post URI (if in thread)
- `thread_parent_post`: Parent post URI (if in thread)
- `quote_post`: Quoted post URI (if quote post)
- `embeddings`: Embedding vectors (map of string to float32 array)
- `indexed_at`: Timestamp when indexed in Elasticsearch

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
