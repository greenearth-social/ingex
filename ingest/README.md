# Ingest Service

Go-based data ingestion services that process BlueSky content from various sources and index them in Elasticsearch for the Green Earth Ingex system.

## Overview

The ingest service suite provides multiple commands for ingesting BlueSky data from different sources:

- **[megastream_ingest](cmd/megastream_ingest/README.md)** - Processes BlueSky posts from Megastream SQLite databases (with embeddings)
- **[jetstream_ingest](cmd/jetstream_ingest/README.md)** - Real-time ingestion of BlueSky "Likes" from the Jetstream WebSocket API

Each command is optimized for its specific data source and use case.

## Features

- **Multiple Data Sources**: Support for Megastream SQLite databases, Jetstream WebSocket API, and more
- **Embedding Support**: Processes pre-computed MiniLM sentence embeddings (megastream_ingest)
- **Real-time Streaming**: WebSocket-based ingestion for live data (jetstream_ingest)
- **Elasticsearch Integration**: Uses [go-elasticsearch](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9) for data indexing
- **Bulk Indexing**: Efficient batch processing for high-throughput ingestion
- **Graceful Shutdown**: Proper SIGTERM handling and context cancellation
- **Structured Logging**: Configurable logging with multiple levels

## Architecture

```text
Data Sources:
  - Megastream SQLite → megastream_ingest → Elasticsearch (posts + tombstones)
  - Jetstream WebSocket → jetstream_ingest → Elasticsearch (likes)
```

### Project Structure

```text
ingest/
├── cmd/
│   ├── elasticsearch_expiry/       # Elasticsearch data expiry job
│   │   ├── main.go                 # CLI and orchestration
│   │   └── README.md               # Expiry-specific documentation
│   ├── megastream_ingest/          # Megastream SQLite ingestion
│   │   ├── main.go                 # CLI and orchestration
│   │   └── README.md               # Megastream-specific documentation
│   └── jetstream_ingest/           # Jetstream WebSocket ingestion
│       ├── main.go                 # CLI and orchestration
│       └── README.md               # Jetstream-specific documentation
├── internal/
│   ├── common/                     # Shared libraries (reusable across services)
│   │   ├── config.go               # Environment-based configuration
│   │   ├── elasticsearch.go        # ES client and bulk operations
│   │   ├── interfaces.go           # Common interfaces
│   │   ├── jetstream_message.go    # Jetstream message parsing
│   │   ├── logger.go               # Structured logging
│   │   ├── message.go              # MegaStream message parsing
│   │   └── state.go                # File processing state management
│   ├── elasticsearch_expiry/       # Expiry-specific implementations
│   │   └── service.go              # Expiry logic
│   ├── megastream_ingest/          # MegaStream-specific implementations
│   │   └── spooler.go              # Local and S3 file discovery/processing
│   └── jetstream_ingest/           # Jetstream-specific implementations
│       └── client.go               # WebSocket client
├── scripts/
│   ├── deploy.sh                                      # Deployment automation
│   ├── gcp_setup.sh                                   # GCP environment setup
│   ├── ingestctl.sh                                   # Control script for ingest services
│   ├── k8s_recreate_api_key.sh                        # Recreate Elasticsearch API key
│   ├── k8s_delete_es_data_via_api.sh                  # Delete ES data via API (safe)
│   ├── k8s_delete_es_data_filesystem_emergency.sh     # Delete ES data from filesystem (emergency only)
│   └── fix_es_readonly.sh                             # Fix ES read-only blocks
├── go.mod                          # Module: github.com/greenearth/ingest
└── test_data/                      # Sample SQLite databases for testing
```

### Core Components

**Shared Components** (`internal/common/`):

- **Message Parsers**: Transform raw data to structured messages (MegaStream, Jetstream)
- **Elasticsearch Client**: Handles indexing with bulk operations for all document types
- **State Manager**: Tracks processed files to avoid duplicates (megastream_ingest)
- **Configuration**: Environment-based config with validation
- **Logger**: Structured logging with configurable output

**Command-Specific Components**:

- **Spooler** (`internal/megastream_ingest/`): Discovers and processes SQLite files from local filesystem or S3
- **WebSocket Client** (`internal/jetstream_ingest/`): Connects to Jetstream and processes real-time events

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

# Build megastream_ingest
go build -o megastream_ingest ./cmd/megastream_ingest

# Build jetstream_ingest
go build -o jetstream_ingest ./cmd/jetstream_ingest

# Or run directly without building
go run ./cmd/megastream_ingest --source local --mode once
go run ./cmd/jetstream_ingest --dry-run
```

See individual command READMEs for detailed usage:

- [megastream_ingest documentation](cmd/megastream_ingest/README.md)
- [jetstream_ingest documentation](cmd/jetstream_ingest/README.md)

## Configuration

Each command has its own configuration requirements. See the individual command READMEs for details:

- [megastream_ingest configuration](cmd/megastream_ingest/README.md#configuration)
- [jetstream_ingest configuration](cmd/jetstream_ingest/README.md#configuration)

### Common Configuration

**All commands require:**

- `ELASTICSEARCH_URL` - Elasticsearch cluster endpoint
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key with appropriate index permissions
- `LOGGING_ENABLED` - Enable/disable logging (default: `true`)

### Getting an Elasticsearch API Key

For local development with Kibana:

1. Access Kibana at <https://localhost:5601> (via port-forward)
2. Navigate to **Stack Management → Security → API Keys**
3. Click **Create API key**
4. Configure with appropriate index permissions (see command-specific docs)
5. Copy the encoded API key

Or via command line:

```bash
# Port-forward to Elasticsearch
kubectl port-forward service/greenearth-es-http 9200 -n greenearth-local

# Get elastic password
ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n greenearth-local -o go-template='{{.data.elastic | base64decode}}')

# Create API key (adjust index names as needed)
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ingest-service-key",
    "expiration": "90d",
    "role_descriptors": {
      "ingest_role": {
        "cluster": ["manage_index_templates", "monitor"],
        "indices": [
          {
            "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1", "likes", "likes_v1", "like_tombstones", "like_tombstones_v1"],
            "privileges": ["create_doc", "create", "delete", "index", "write", "maintenance", "all"]
          }
        ]
      }
    }
  }'
```

Use the `encoded` value from the response.

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

## Deployment

### Local Testing

Run against local Elasticsearch cluster (see [../index/README.md](../index/README.md)):

```bash
# Start port-forward to local Elasticsearch
kubectl port-forward service/greenearth-es-http 9200 -n greenearth-local

# Run megastream_ingest
./megastream_ingest --source local --mode once --skip-tls-verify

# Or run jetstream_ingest
./jetstream_ingest --skip-tls-verify
```

See individual command READMEs for detailed deployment instructions.

### Production Deployment

- **Target Platform**: (TODO) Azure Kubernetes Service (AKS)
- **Container Runtime**: (TODO) Docker with multi-stage builds
- **Deployment Method**: (TODO) Kubernetes manifests via Terraform
- **Monitoring**: (TODO) Add health checks and metrics endpoints

## Development

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
    "github.com/greenearth/ingest/internal/jetstream_ingest"
)
```

### Testing and Linting

Install golangci-lint:

```bash
# macOS
brew install golangci-lint

# Linux
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin

# Or using go install (slower, not recommended by package authors)
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

Run locally:

```bash
cd ingest

# Run all tests with race detector
go test -v -race ./...

# Run linter
golangci-lint run

# Run tests with coverage
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

**VS Code Setup**: Open the VScode settings, find the golang linter, and select `golangci-lint-v2` from the dropdown.

This enables real-time linting in the editor using the project's `.golangci.yml` configuration.

### CI

The project uses GitHub Actions for continuous integration:

- **Tests**: Runs on push/PR with race detector and coverage
- **Linting**: golangci-lint with static analysis
- **Build**: Validates both binaries compile successfully

See `.github/workflows/go-ci.yml` for CI configuration.

### Integration Testing

See individual command READMEs for command-specific integration testing:

- [megastream_ingest testing](cmd/megastream_ingest/README.md#testing)
- [jetstream_ingest testing](cmd/jetstream_ingest/README.md#building)

## Elasticsearch Indexes

The ingest services write to the following Elasticsearch indexes:

### Posts (`posts` alias → `posts_v1`)

BlueSky posts with full content and embeddings (from megastream_ingest):

- `at_uri` - AT Protocol URI
- `author_did` - Author's DID
- `content` - Post text
- `created_at` - Creation timestamp
- `thread_root_post`, `thread_parent_post`, `quote_post` - Relationship URIs
- `embeddings` - Sentence embeddings (MiniLM-L6-v2, MiniLM-L12-v2)
- `indexed_at` - Indexing timestamp

### Post Tombstones (`post_tombstones` alias → `post_tombstones_v1`)

Deleted post records (from megastream_ingest):

- `at_uri` - AT Protocol URI of deleted post
- `author_did` - Author's DID
- `deleted_at` - Deletion timestamp
- `indexed_at` - Indexing timestamp

### Likes (`likes` alias → `likes_v1`)

BlueSky like events (from jetstream_ingest):

- `uri` - AT Protocol URI of the like
- `subject_uri` - URI of the post being liked
- `author_did` - DID of user who liked
- `created_at` - Like creation timestamp
- `indexed_at` - Indexing timestamp

See [../index/README.md](../index/README.md) for Elasticsearch infrastructure setup and index template details.
