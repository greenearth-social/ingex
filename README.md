# Green Earth Ingex

A data ingestion and indexing system for BlueSky content. This project provides real-time streaming or batch processing capabilities to capture, process, and search BlueSky posts and likes in an ElasticSearch backend.

## Contributing

Interested in contributing? We'd love to have you! 

First, please join our discord and introduce yourself: https://discord.com/invite/8bWEyrkrJC. Unless you've joined the discord and engaged with the community there, all issues/PRs will be auto-closed.

## System Architecture

For detailed architecture information, see [VPC_ARCHITECTURE.md](VPC_ARCHITECTURE.md).

### Data Ingestion

- **Posts**
  - Processed in batch sqlite files from [Graze Megastream](https://graze.leaflet.pub/3m33mkloj222o)
- **Like**
  - Processed in realtime from [Bluesky JetStream](https://docs.bsky.app/blog/jetstream)
- **Runtime**: Deployed on [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/docs)
- **Client Library**: [go-elasticsearch](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9) for connecting to ES and data indexing
- **Documentation**: See [ingest/README.md](ingest/README.md) for development and deployment instructions

### Search & Indexing

- **Search Engine**: [Elasticsearch](https://www.elastic.co/docs/solutions/search) for full-text search and analytics
- **Infrastructure**: [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/docs) for hosting
- **Documentation**: See [index/README.md](index/README.md) for deployment and testing instructions

## Development & Deployment

### Repository Structure

- `/ingest` - All code related to the Go-based ingestion service.
- `/index` - All code related to the Elastic Search index and query service.

### Continuous Integration (Github Actions)

- **Testing**: Go test suites on all PRs
- **Quality Assurance**: Automated linting, formatting, and security checks

## Index Schema Migrations

When the Elasticsearch index mappings change (e.g. adding or removing fields, changing `index` flags, changing shard counts, or dropping HNSW graphs), existing indices must be reindexed — ILM templates only apply to newly created indices.

Use `tools/reindex.py` to migrate live indices. The script reindexes each source index into a new destination named `<index>-<commit>`, atomically swaps all aliases, and deletes the source. It supports two independent operations that can be run separately or together:

- **`--migrate`** — reindex + alias swap
- **`--force-merge`** — reduce each index to 1 Lucene segment (reduces per-shard term-seek cost ~30×; heavy I/O, run off-peak)

### Setup

```bash
cd tools
pipenv install
```

### Prerequisites

Before running a migration:

1. Deploy the updated ILM index templates (runs automatically via the deploy script's bootstrap job, or manually with `kubectl apply`).
2. Deploy the new ingest service version so new documents are written in the updated format.

### Credentials

Export ES credentials (or set them in your shell environment):

```bash
export GE_ELASTICSEARCH_URL=https://<host>:9200
export GE_ELASTICSEARCH_USERNAME=elastic          # default: elastic
export GE_ELASTICSEARCH_PASSWORD=<elastic-password>
export GE_ELASTICSEARCH_TLS_SKIP_VERIFY=true      # stage only (self-signed cert)
```

The `elastic` superuser has full cluster privileges and can read all aliases, which is required for the active-index safety check.

### Migration workflow

The active write index is skipped by default. The typical workflow for a full migration is two passes:

**Pass 1 — migrate all historical indices** (the active write index is skipped automatically):

```bash
cd tools
pipenv run python reindex.py --migrate --types posts replies --dry-run  # preview
pipenv run python reindex.py --migrate --types posts replies
```

**Pass 2 — after the period rolls over**, the formerly-active index is now historical. Target it by name with `--indices` to complete the migration:

```bash
# Identify the formerly-active index (e.g. posts-2026-w26)
pipenv run python reindex.py --migrate --indices posts-2026-w26 replies-2026-w26
```

`--indices` bypasses the active-index guard entirely, so no confirmation prompt is needed. This is the safe and recommended way to handle the formerly-active index after rollover.

### Force-merge

Force-merging reduces each shard's Lucene segments to 1, which cuts per-shard term-seek overhead ~30× and improves query latency on historical (read-only) indices. Run it off-peak — it is I/O-intensive.

```bash
# Standalone: submit all force-merges async, then poll with progress
pipenv run python reindex.py --force-merge --types posts replies

# Target specific indices by name
pipenv run python reindex.py --force-merge --indices posts-2026-w25-abc1234
```

Combined with `--migrate`, force-merge fires automatically after each alias swap (fire-and-forget — not waited on, runs concurrently with the next index migration):

```bash
pipenv run python reindex.py --migrate --force-merge --types posts replies
```

### `--include-active` (last resort)

`--include-active` reindexes the index currently receiving live writes. **Only use this when all ingest services have been stopped** — documents written after reindexing starts but before the alias swap completes are permanently lost. Prefer the two-pass workflow with `--indices` after rollover instead.

```bash
# Prompts for explicit confirmation before proceeding
pipenv run python reindex.py --migrate --types posts replies --include-active
```

### Resuming after interruption

State is written to `tools/state/reindex-state.json` after every step. If the script is interrupted or fails, re-run with the same flags and the same git commit in the working tree — it will skip completed indices and resume from where it left off. In-flight Elasticsearch reindex tasks continue running server-side and are re-attached automatically on resume.

To discard saved state and start the migration from scratch:

```bash
pipenv run python reindex.py --migrate --types posts replies --reset
```

### Adding new index types

Register the new type in the `INDEX_TYPES` dict at the top of `tools/reindex.py`:

```python
INDEX_TYPES = {
    ...
    "likes": {
        "pattern": "likes-*",
        "active_alias": "likes",
    },
}
```

The script will handle discovery, reindexing, alias swaps, and state tracking automatically.

---

## Working with staging and production clusters

To set up kubectl to point at the remote control plane, do this:

```bash
gcloud container clusters get-credentials "greenearth-stage-cluster" --region "us-east1"
```

You may need to run `gcloud components install gke-gcloud-auth-plugin` first.

To see which cluster you're pointed at, run `kubectl cluster-info`
