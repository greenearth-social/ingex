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
- `JETSTREAM_STATE_FILE` - Path to state file for cursor tracking (default: `.jetstream_state.json`)

## Usage

```bash
# Basic usage
./jetstream_ingest
```

## Command Line Flags

- `-dry-run` - Run without writing to Elasticsearch
- `-skip-tls-verify` - Skip TLS certificate verification (use for local development only)
- `-no-rewind` - Do not rewind to the last processed timestamp

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

### Use of the Jetstream cursor

By default, the service will use the Jetstream cursor to rewind to the last processed timestamp. This helps to
guarantee that we don't miss any data.

## Notes

- The service only processes "Like" events. Other event types from the Jetstream are ignored.
- Connection failures trigger automatic reconnection with logging for visibility.
- Starting the service with rewind enabled (default) might result in processing a large number of entries
  very quickly, as it catches up.

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
