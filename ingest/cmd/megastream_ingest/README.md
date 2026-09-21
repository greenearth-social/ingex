# Megastream Ingest

This command processes BlueSky content from Megastream SQLite databases and indexes them in Elasticsearch.

## Overview

The `megastream_ingest` command:

- Reads JSON-formatted, hydrated BlueSky posts with sentence embeddings from Megastream SQLite databases
- Supports both local filesystem and S3 as data sources
- Handles both one-time ingestion and continuous monitoring (spool mode)
- Batches posts and indexes them to Elasticsearch (`posts` index)
- Also indexes per-post inference data (sentiment, toxicity, topic, embeddings, etc.) to the `inferences` index
- Tracks processed files to avoid duplicates
- Provides graceful shutdown handling

## Topic scores

`topic_scores` contains topic-classification scores computed upstream by Graze,
not by ingex. We extract them from the Megastream inference JSON at:

```text
inferences["text"]["message.commit.record.text"]["topic"]
```

Only the post-body analysis is used. Embedded-link titles and descriptions can
have their own topic analyses, but those are not substituted for body scores.

Model documentation (checked September 9, 2026):

- [Graze editor documentation](https://www.graze.social/docs/editor-documentation),
  under **Topic Analysis**, lists the 19 topic categories, including
  `News & Social Concern`, and links to the model below.
- [CardiffNLP tweet-topic model card](https://huggingface.co/cardiffnlp/twitter-roberta-base-dec2021-tweet-topic-multi-all)
  documents `cardiffnlp/twitter-roberta-base-dec2021-tweet-topic-multi-all`,
  including its training data and multilabel classification usage.

These links identify Graze's documented topic-analysis model, not a guaranteed
model revision for every Megastream record. Ingex consumes the supplied scores
and does not run, pin, or verify the upstream model version.

Ingex retains all supplied finite numeric scores in `[0, 1]`, without applying a
positive-label threshold. Invalid entries (including explicit `null` values) are
dropped and counted, with rate-limited error summaries. An absent body-topic
analysis is not an error; for example, an empty-body post containing only an
image or link may have no body scores. Missing scores are not replaced with zero.

## Configuration

Configuration is done through environment variables and command line flags.

### Command Line Flags

- `--source` - Source of SQLite files: `local` or `s3` (default: `local`)
- `--mode` - Ingestion mode: `once` (single run) or `spool` (continuous polling) (default: `once`)
- `--dry-run` - Run without writing to Elasticsearch (for testing)
- `--skip-tls-verify` - Skip TLS certificate verification (local development only)
- `--no-rewind` - Do not rewind to the last processed timestamp on startup (drops intervening data)
- `--no-perspective` - Skip Perspective API scoring of posts

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

**Post-Tower Embeddings (optional):**

When `GE_INFERENCE_BASE_URL` is set, the service calls the inference service's
post tower for each new non-reply post (inputs: the post's
`all_MiniLM_L12_v2` content embedding and author DID) and stores the resulting
128-dim post embedding on the posts index as `embeddings.ge_post_embedding`.
Failures are fail-open: posts are still indexed without the field. Disabled in
`--dry-run` mode.

- `GE_INFERENCE_BASE_URL` - Inference service base URL (e.g. `https://inference-stage.greenearth.social`); unset disables post embeddings
- `GE_INFERENCE_API_KEY` - Inference service API key (GSM secret `inference-api-key-{env}`)
- `GE_INFERENCE_TIMEOUT` - Per-request HTTP timeout (default: `10s`)
- `GE_INFERENCE_CHUNK_SIZE` - Posts per inference request; must not exceed the server's `GE_INFERENCE_MAX_BATCH` (default: `1024`)
- `GE_INFERENCE_MAX_CONCURRENCY` - Concurrent inference requests (default: `8`)
- `GE_INFERENCE_RETRY_MAX` - Retries beyond the first attempt for transient failures (default: `3`)

> **Rollout ordering:** the `ge_post_embedding` dense_vector mapping must be
> deployed to the posts index template (`index/deploy.sh <env> --ctypes schema`,
> then wait for the next index rollover or PUT the mapping onto the current
> write index) *before* deploying this service with `GE_INFERENCE_BASE_URL`
> set. Otherwise Elasticsearch dynamically maps the field as `float`, which
> cannot serve kNN queries and requires a reindex to fix.

**Perspective Scoring (optional):**

When `GE_PERSPECTIVE_API_KEY` is set, each new non-reply post is scored by
Google's Perspective API and the result is stored on the posts index as
`perspective_scores` (the 15 raw PRC attribute scores), `combined_perspective_score`
(the weighted PRC score in `[0, 1]`), and `perspective_scored_at`.

This exists so the api does not have to score candidates on the serving path,
and so we accumulate a complete attribute-score corpus before the Perspective
API sunsets in January — see greenearth-social/api#368. Replies are not scored.

Three states are meaningful, and the api distinguishes all three:

| document state | meaning |
|---|---|
| all three fields set | scored |
| `perspective_scored_at` only | permanently unscorable — no text at all (an image-only post), or a language the API declines to rate. Never retried. |
| no fields | not scored yet. The api scores it live; `backfill_perspective` fills it in. |

`backfill_perspective` writes to the concrete index each post lives in, taken
from the scan's `_index`, not to `--source-index`. That flag is normally the
`posts_recent` alias, and a bulk update addressed to an alias is routed to the
alias's *write* index — so with a weekly index per retention period, every post
outside the current week would come back `document_missing`, which the write
path counts as routine. Watch `es.update_perspective_scores.missing_count`
against `updated_count`: routine one at a time, a misroute in bulk.

The middle state matters more than it looks: without it, every image-only and
non-English post would be re-submitted on every backfill run and re-queried by
the api on every feed request, forever.

Failures are fail-open: posts are still indexed with no perspective fields.
Disabled in `--dry-run` mode and by `--no-perspective`, and gated on the
destination index mapping the fields (below).

- `GE_PERSPECTIVE_API_KEY` - Perspective API key (GSM secret `perspective-api-key-{env}`); unset disables scoring
- `GE_PERSPECTIVE_HOST` - API host override (default: `https://commentanalyzer.googleapis.com`); the devenv points this at its local stub
- `GE_PERSPECTIVE_QPS` - This service's share of the shared quota (default: `150` in prod, `15` in stage)
- `GE_PERSPECTIVE_ON_QUOTA` - `wait` to throttle ingest, `skip` to index posts unscored (default: `wait`)
- `GE_PERSPECTIVE_TIMEOUT` - Per-request HTTP timeout (default: `2s`)
- `GE_PERSPECTIVE_MAX_CONCURRENCY` - Concurrent scoring requests (default: `32`)
- `GE_PERSPECTIVE_RETRY_MAX` - Retries beyond the first attempt (default: `2`)

> **Quota is shared.** The Perspective quota is 36 000 requests/minute (600 QPS)
> across *both* this service and the api's serving path. `GE_PERSPECTIVE_QPS` is
> ingest's slice of it — 9 000 RPM at the default, against serving's 26 700
> (`GE_PERSPECTIVE_QPM` in the api), summing to 35 700 so that 300 RPM stays
> unclaimed. That buffer covers the inexactness of two independent limiters in
> separate processes; raising either slice without lowering the other spends it.
> Serving also sees spikes ingest does not. `wait` keeps ingest inside its slice by slowing
> it down; switch to `skip` when serving needs the budget more than the corpus
> does, then recover the gap with `backfill_perspective`.
>
> **Stage draws on the same pool.** Both environments deploy into one GCP
> project and Perspective quota is per project, so those two slices are really
> four claims on one 36 000. Stage's ingest ceiling is scaled by the sample rate
> instead: `ShouldSampleDID` keeps 1 DID in 10 there, so stage ingests a tenth
> of the stream and its default is a tenth of prod's, `15`. That is a ceiling,
> not a reservation — stage's real draw is whatever a tenth of the stream costs.
> The point of the lower number is that a regression which made stage spend
> prod-sized budget gets throttled instead of quietly eating serving's headroom.
> Keep it in step with `ingestSampleDenominator`.

> **Rollout ordering:** as with `ge_post_embedding`, deploy the posts index
> template first — but the service enforces this rather than trusting it.
>
> An index template applies only when an index is *created*, so the period
> index already being written to when the template lands does not have the new
> fields, and the template alone cannot give them to it. Writing anyway would
> let Elasticsearch infer the types from whichever document arrives first, and
> Go renders `float64(0)` as the JSON integer `0`: a maximally toxic post
> arriving first maps `combined_perspective_score` as a `long`, after which
> every fractional score is silently truncated to `0` in the index — accepted,
> not rejected — and a field's type cannot be changed in place. `_source` keeps
> the true value so serving still reads the right number, but the field is
> mapped `index: true` so that it can be queried and aggregated, and that would
> quietly return nonsense.
>
> So scoring is **gated on the mapping**: before writing, the service checks
> that the current period index maps `combined_perspective_score`,
> `perspective_scored_at` and `perspective_scores.*` as the template declares
> them. While it does not, posts are indexed unscored — the same state as never
> having been scored, so the api scores them live and `backfill_perspective`
> can collect them later. The check re-runs each minute alongside `EnsureIndex`,
> so a deploy landing mid-period needs no intervention: the next period index is
> created from the current template and scoring starts at the boundary.
>
> `perspective.index_gate.ready` is `1` when scoring is live and `0` while
> gated. Expect `0` for the remainder of the period after a mid-period deploy;
> `0` past the following boundary means the template never landed.

**Metrics.** `perspective.rate_limit.wait_ms` rises first when the budget starts
binding; `perspective.rate_limit.throttled.count` shows how often. A non-zero
`perspective.rate_limit.skipped.count` means posts were indexed unscored and a
`backfill_perspective` run is owed. `perspective.index_gate.ready` at `0` means
nothing is being scored at all — see rollout ordering above.

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

Chunks arrive in the bucket at roughly one per five minutes, ~115 MB and
~4,000 rows each (~800 chunks/day). Adjacent chunks can repeat an `at_uri` —
typically a post created in one and deleted in the next — so consumers that
aggregate across chunks should deduplicate.

Despite the `.db.zip` suffix, current chunks are **raw SQLite**; older
archived chunks are real zip archives. The spooler sniffs the magic bytes
(`isZipFile` in `internal/megastream_ingest/spooler.go`) and handles both, so
the suffix alone tells you nothing about the encoding.

Each file holds an `enriched_posts` table:

- `id` - autoincrement row id
- `at_uri` - AT Protocol URI for the post
- `did` - Decentralized Identifier of the author
- `time_us` - jetstream event timestamp, microseconds
- `raw_post` - JSON blob with post data (see below)
- `inferences` - JSON blob with embeddings and per-post model outputs
- `enriched_metadata` - JSON blob recording which models ran
- `created_at` - when the row was written

### What we read, and what we leave behind

`internal/common/megastream_message.go` extracts roughly fifteen fields and
discards the rest. The unread remainder is substantial, and worth knowing
about before adding a feature that assumes we'd have to go fetch it:

- `raw_post.hydrated_metadata.user` - the author's full
  `profileViewDetailed`: `labels[]` (moderation and self-labels applied to the
  account), `followers_count`, `follows_count`, `posts_count`, `created_at`,
  `handle`, `description`. None of it reaches Elasticsearch.
- `raw_post.message.commit.record.langs` - declared post languages. About 27%
  of posts declare something other than `en`.
- `raw_post.message.commit.record.labels` - author **self**-labels on the post
  (`porn`, `sexual`, `graphic-media`, `nudity`).
- `inferences.text.<field-path>.*` - per-post classifier outputs keyed by which
  text was analyzed: `moderation`, `toxicity`, `marketing_check`, `topic`,
  `sentiment`, `emotion_sentiment`, `financial_sentiment`,
  `language_detection`, `text_arbitrary`. Only the embeddings are ingested.

`scripts/megastream_drop_analysis.py` reads these directly from the chunks and
reports what share of ingested posts they would let us skip. Note that
`marketing_check` did not discriminate spam in sampling — see that script's
docstring before relying on it.

### Fetching chunks by hand

The bucket is **requester-pays**, so `aws` calls need `--request-payer
requester` (the spooler passes `RequestPayer: "requester"` for the same
reason) and the transfer is billed to us — about 90 GB for a full day.

```bash
set -a && . ingest/.env && set +a   # AWS credentials
aws s3api list-objects-v2 --bucket graze-mega-02 --prefix mega/ \
  --request-payer requester --query 'Contents[*].Key' --output text | sort | tail -5
```
