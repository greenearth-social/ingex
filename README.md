# Green Earth Ingex

A data ingestion and indexing system for BlueSky content. This project provides real-time streaming or batch processing capabilities to capture, process, and search BlueSky posts and likes in an ElasticSearch backend.

## System Architecture

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

## Working with staging and production clusters

To set up kubectl to point at the remote control plane, do this:

```bash
gcloud container clusters get-credentials "greenearth-stage-cluster" --region "us-east1"
```

You may need to run `gcloud components install gke-gcloud-auth-plugin` first.

To see which cluster you're pointed at, run `kubectl cluster-info`

