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

- `GE_JETSTREAM_URL` - WebSocket URL for Jetstream (default: `wss://jetstream2.us-east.bsky.network/subscribe`)
- `GE_ELASTICSEARCH_URL` - Elasticsearch cluster URL
- `GE_ELASTICSEARCH_API_KEY` - Elasticsearch API key (not required in dry-run mode)

### Optional

- `GE_LOGGING_ENABLED` - Enable detailed logging (default: `true`)
- `GE_JETSTREAM_STATE_FILE` - Path to state file for cursor tracking (default: `.jetstream_state.json`)
- `GE_LIKE_COUNT_FLUSH_INTERVAL` - How often held like counts for popular posts are written (default: `10m`; `0` writes every like immediately)
- `GE_LIKE_COUNT_EXACT_BELOW` - Posts with fewer likes than this are written on every like (default: `10`; keep at or above `GE_QUALITY_LIKE_THRESHOLD`)
- `GE_LIKE_COUNT_CACHE_SIZE` - How many posts' like counts are remembered to make that decision (default: `200000`)

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

### Like Count Coalescing

Every like-count update re-indexes the whole post, including its embedding vectors, so writing each like as it arrives is the most expensive part of like ingestion. Posts with fewer than `GE_LIKE_COUNT_EXACT_BELOW` likes, or whose count the service hasn't seen yet, are still written on every like, which keeps small counts exact and quality-corpus promotion immediate. More popular posts have their likes summed in memory and written every `GE_LIKE_COUNT_FLUSH_INTERVAL`.

Held counts are written on shutdown, but anything that doesn't finish within the shutdown window is lost: the cursor has already moved past those likes. Popular posts can therefore drift low by up to one interval's likes per restart, which is accepted.

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
export GE_JETSTREAM_URL="wss://jetstream2.us-east.bsky.network/subscribe"
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key"
export GE_LOGGING_ENABLED="true"

./jetstream_ingest
```
