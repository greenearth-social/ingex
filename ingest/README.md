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

```
Megastream SQLite → Data Reader → Document Mapper → Elasticsearch Client → Elasticsearch
                         ↓              ↓                      ↓
                   Row Processing  JSON Extraction      Bulk Operations
```

### Core Components

- **SQLite Reader**: Processes enriched_posts table from Megastream databases
- **Document Mapper**: Transforms SQLite rows to Elasticsearch documents
- **Elasticsearch Client**: Handles indexing with bulk operations
- **Configuration Management**: Environment-based config with validation
- **Logger Interface**: Structured logging with multiple implementations

## Local Development

### Prerequisites
- Go 1.21+
- Access to Elasticsearch cluster (see [../index/README.md](../index/README.md) for local setup)

### Quick Start

```bash
# Install dependencies
go mod download

# Run tests
go test -v

# Build the service
go build -o ingest

# Run locally (requires environment variables)
./ingest
```

## Configuration

### Environment Variables

- `SQLITE_DB_PATH` - Path to Megastream SQLite database file (required)
- `ELASTICSEARCH_URL` - Elasticsearch cluster endpoint (required)
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key with permissions described below (required)
```
"indices": [
      {
      "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1"],
      "privileges": ["create_doc", "create", "delete", "index", "write", "all"]
      }
]
```
- `LOGGING_ENABLED` - Enable/disable logging (default: true)

### Example Configuration

```bash
export SQLITE_DB_PATH="/path/to/megastream/mega_jetstream_20250909_204657.db"
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_API_KEY="asdvnasdfdsa=="
export LOGGING_ENABLED="true"
```

## Deployment

### Local Testing
Run against local Elasticsearch cluster (see [../index/README.md](../index/README.md)):

```bash
# Start port-forward to local Elasticsearch
kubectl port-forward service/greenearth-es-local-es-http 9200 -n greenearth-local

./ingest --skip-tls-verify
```

### Production Deployment
- **Target Platform**: (TODO) Azure Kubernetes Service (AKS)
- **Container Runtime**: (TODO) Docker with multi-stage builds
- **Deployment Method**: (TODO)) Kubernetes manifests via Terraform
- **Monitoring**: (TODO) Add health checks and metrics endpoints