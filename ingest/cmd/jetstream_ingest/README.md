# Jetstream Ingest

This command connects to the Bluesky Jetstream WebSocket API and ingests "Like" events into Elasticsearch.

## Overview

The `jetstream_ingest` command:

- Connects to the Bluesky Jetstream WebSocket API
- Filters for `app.bsky.feed.like` events
- Batches likes and indexes them to Elasticsearch
- Supports automatic reconnection on connection failures
- Provides graceful shutdown handling

## Configuration

Configuration is done through environment variables:

### Required

- `JETSTREAM_URL` - WebSocket URL for Jetstream (default: `wss://jetstream2.us-east.bsky.network/subscribe`)
- `ELASTICSEARCH_URL` - Elasticsearch cluster URL
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key (not required in dry-run mode)

### Optional

- `LOGGING_ENABLED` - Enable detailed logging (default: `true`)

## Usage

```bash
# Basic usage
./jetstream_ingest

# Dry-run mode (no writes to Elasticsearch)
./jetstream_ingest -dry-run

# Skip TLS verification (local development only)
./jetstream_ingest -skip-tls-verify
```

## Command Line Flags

- `-dry-run` - Run without writing to Elasticsearch
- `-skip-tls-verify` - Skip TLS certificate verification (use for local development only)

## Elasticsearch Index

Likes are indexed to the `likes` index with the following structure:

```json
{
  "uri": "at://did:plc:xxxxx/app.bsky.feed.like/xxxxx",
  "subject_uri": "at://did:plc:yyyyy/app.bsky.feed.post/zzzzz",
  "author_did": "did:plc:xxxxx",
  "created_at": "2025-10-30T12:34:56.789Z",
  "indexed_at": "2025-10-30T12:34:57.123Z"
}
```

## Features

### Automatic Reconnection

The client automatically reconnects if the WebSocket connection is lost, with a 5-second backoff between attempts.

### Batch Processing

Likes are batched and indexed in groups of 100 to optimize Elasticsearch performance.

### Graceful Shutdown

The service responds to SIGINT and SIGTERM signals, completing the current batch before shutting down.

## Notes

- This service does not maintain cursor state. If the service is restarted, it will start receiving new events from that point forward. Historical likes will not be retrieved.
- The service only processes "Like" events. Other event types from the Jetstream are ignored.
- Connection failures trigger automatic reconnection with logging for visibility.

## Building

```bash
go build -o jetstream_ingest cmd/jetstream_ingest/main.go
```

## Example

```bash
export JETSTREAM_URL="wss://jetstream2.us-east.bsky.network/subscribe"
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
export LOGGING_ENABLED="true"

./jetstream_ingest
```
