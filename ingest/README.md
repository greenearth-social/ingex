# Ingest Service

Go-based data ingestion service that processes BlueSky content from Megastream SQLite databases and indexes them in Elasticsearch for the Green Earth Ingex system.

## Overview

The ingest service reads JSON-formatted, hydrated BlueSky content with sentence embeddings from SQLite database files provided by Megastream, then indexes this content into Elasticsearch for search and analysis.

**Future Direction**: This service will support multiple data sources including websocket streams, local SQLite files, and remote SQLite files hosted on S3.

## Features

- **SQLite Data Processing**: Reads enriched BlueSky posts from Megastream SQLite databases
- **Embedding Support**: Processes pre-computed MiniLM sentence embeddings (L6-v2 and L12-v2 models)
- **Elasticsearch Integration**: Uses [go-elasticsearch](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9) for data indexing
- **Bulk Indexing**: Efficient batch processing for high-throughput ingestion
- **Data Mapping**: Transforms Megastream schema to Elasticsearch document structure
- **Graceful Shutdown**: Proper SIGTERM handling and context cancellation
- **Structured Logging**: Configurable logging with multiple levels

## Architecture

```text
Megastream SQLite → Data Reader → Document Mapper → Elasticsearch Client → Elasticsearch
                         ↓              ↓                      ↓
                   Row Processing  JSON Extraction      Bulk Operations
```

### Project Structure

```text
ingest/
├── cmd/
│   └── megastream_ingest/          # Main application entry point
│       └── main.go                 # CLI and ingestion orchestration
├── internal/
│   ├── common/                     # Shared libraries (reusable across services)
│   │   ├── config.go               # Environment-based configuration
│   │   ├── elasticsearch.go        # ES client and bulk operations
│   │   ├── interfaces.go           # Common interfaces
│   │   ├── logger.go               # Structured logging
│   │   ├── message.go              # MegaStream message parsing
│   │   └── state.go                # File processing state management
│   └── megastream_ingest/          # MegaStream-specific implementations
│       └── spooler.go              # Local and S3 file discovery/processing
├── go.mod                          # Module: github.com/greenearth/ingest
└── test_data/                      # Sample SQLite databases for testing
```

### Core Components

- **Spooler** (`internal/megastream_ingest/spooler.go`): Discovers and processes SQLite files from local filesystem or S3
- **Message Parser** (`internal/common/message.go`): Transforms MegaStream SQLite rows to structured messages
- **Elasticsearch Client** (`internal/common/elasticsearch.go`): Handles indexing with bulk operations
- **State Manager** (`internal/common/state.go`): Tracks processed files to avoid duplicates
- **Configuration** (`internal/common/config.go`): Environment-based config with validation
- **Logger** (`internal/common/logger.go`): Structured logging with configurable output

## Local Development

### Prerequisites

- Go 1.21+
- Access to Elasticsearch cluster (see [../index/README.md](../index/README.md) for local setup)

### Quick Start

```bash
# Install dependencies
go mod download

# Run tests for common libraries
go test ./internal/common -v

# Build the service
go build -o megastream_ingest ./cmd/megastream_ingest

# Run locally with local SQLite files (requires environment variables)
./megastream_ingest --source local --mode once

# Or run directly without building
go run ./cmd/megastream_ingest --source local --mode once
```

## Configuration

### Command Line Flags

- `--source` - Source of SQLite files: `local` or `s3` (default: "local")
- `--mode` - Ingestion mode: `once` (single run) or `spool` (continuous polling) (default: "once")
- `--dry-run` - Run without writing to Elasticsearch (for testing)
- `--skip-tls-verify` - Skip TLS certificate verification (local development only)

### Environment Variables

**Required:**

- `ELASTICSEARCH_URL` - Elasticsearch cluster endpoint
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key with permissions described below

```json
"indices": [
      {
      "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1"],
      "privileges": ["create_doc", "create", "delete", "index", "write", "all"]
      }
]
```

**For Local Source (`--source local`):**

- `LOCAL_SQLITE_DB_PATH` - Directory containing `.db.zip` files to process

**For S3 Source (`--source s3`):**

- `S3_SQLITE_DB_BUCKET` - S3 bucket name containing SQLite files
- `S3_SQLITE_DB_PREFIX` - S3 key prefix (folder path)
- `AWS_REGION` - AWS region (default: "us-east-1")

**Optional:**

- `LOGGING_ENABLED` - Enable/disable logging (default: true)
- `SPOOL_INTERVAL_SEC` - Polling interval in seconds for spool mode (default: 60)
- `SPOOL_STATE_FILE` - Path to state file for tracking processed files (default: ".processed_files.json")

### Example Configuration

**Local Source:**

```bash
export LOCAL_SQLITE_DB_PATH="/path/to/megastream/"
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_API_KEY="asdvnasdfdsa=="
export LOGGING_ENABLED="true"

./megastream_ingest --source local --mode once --skip-tls-verify
```

**S3 Source:**

```bash
export S3_SQLITE_DB_BUCKET="my-bucket"
export S3_SQLITE_DB_PREFIX="megastream/databases/"
export AWS_REGION="us-west-2"
export ELASTICSEARCH_URL="https://my-cluster.es.amazonaws.com:9200"
export ELASTICSEARCH_API_KEY="asdvnasdfdsa=="

./megastream_ingest --source s3 --mode spool
```

## Testing

### Unit Tests

Run tests for common libraries:

```bash
# Test configuration
go test ./internal/common -run TestConfig -v

# Test logger
go test ./internal/common -run TestLogger -v

# Test state manager
go test ./internal/common -run TestState -v

# Test Elasticsearch operations
go test ./internal/common -run TestElasticsearch -v

# Run all tests
go test ./internal/common/... -v
```

### Integration Testing

Test with sample data:

```bash
# Set up test environment
export LOCAL_SQLITE_DB_PATH="./test_data"
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_API_KEY="your-api-key"

# Run in dry-run mode (no ES writes)
go run ./cmd/megastream_ingest --source local --mode once --dry-run

# Run against local ES cluster
go run ./cmd/megastream_ingest --source local --mode once --skip-tls-verify
```

## Deployment

### Local Testing

Run against local Elasticsearch cluster (see [../index/README.md](../index/README.md)):

```bash
# Start port-forward to local Elasticsearch
kubectl port-forward service/greenearth-es-local-es-http 9200 -n greenearth-local

# Build and run
go build -o ingest ./cmd/megastream_ingest
./megastream_ingest --source local --mode once --skip-tls-verify
```

### Continuous Ingestion (Spool Mode)

For continuous monitoring and processing of new files:

```bash
export SPOOL_INTERVAL_SEC="300"  # Check every 5 minutes

# Local source
./megastream_ingest --source local --mode spool

# S3 source
./megastream_ingest --source s3 --mode spool
```

The spooler maintains a state file (`.processed_files.json`) to track which files have been processed and avoid duplicates.

### Production Deployment

- **Target Platform**: (TODO) Azure Kubernetes Service (AKS)
- **Container Runtime**: (TODO) Docker with multi-stage builds
- **Deployment Method**: (TODO) Kubernetes manifests via Terraform
- **Monitoring**: (TODO) Add health checks and metrics endpoints

## Development

### Adding New Data Sources

To add a new data source (e.g., WebSocket, Kafka):

1. Implement the `Spooler` interface in a new package under `internal/`
2. Add configuration options in `internal/common/config.go`
3. Update `cmd/megastream_ingest/main.go` to initialize the new spooler
4. Follow the existing pattern from `megastream_ingest/spooler.go`

### Code Organization

- **`cmd/`**: Executable entry points (one per command/service)
- **`internal/common/`**: Shared libraries usable across multiple services
- **`internal/<service>/`**: Service-specific implementations
- **Test files**: Co-located with source files (`*_test.go`)

### Import Paths

All internal imports use the full module path from `go.mod`:

```go
import (
    "github.com/greenearth/ingest/internal/common"
    "github.com/greenearth/ingest/internal/megastream_ingest"
)
```

