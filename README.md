# Green Earth Ingex

A data ingestion and indexing system for BlueSky content. This project provides real-time streaming or batch processing capabilities to capture, process, and search BlueSky posts and likes in an ElasticSearch backend.

## System Architecture

### Data Ingestion

- **Posts**
  - Processed in batch sqlite files from [Graze Megastream](https://graze.leaflet.pub/3m33mkloj222o)
- **Like**
  - (TODO) Processed in realtime from [Bluesky JetStream](https://docs.bsky.app/blog/jetstream)
- **Runtime**: (TODO) Deployed on [Azure Kubernetes Service](https://azure.microsoft.com/en-us/products/kubernetes-service/) in production, temporarily at [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/docs) for testing
- **Client Library**: [go-elasticsearch](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9) for connecting to ES and data indexing
- **Documentation**: See [ingest/README.md](ingest/README.md) for development and deployment instructions

### Search & Indexing

- **Search Engine**: [Elasticsearch](https://www.elastic.co/docs/solutions/search) for full-text search and analytics
- **Infrastructure**: [Elastic Cloud on Kubernetes (ECK)](https://www.elastic.co/docs/deploy-manage/deploy/cloud-on-k8s#eck-overview) running on [Azure Kubernetes Service](https://azure.microsoft.com/en-us/products/kubernetes-service/) in production, temporarily at [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/docs) for testing
- **Documentation**: See [index/README.md](index/README.md) for deployment and testing instructions

## Development & Deployment

### Repository Structure

- `/ingest` - All code related to the Go-based ingestion service.
- `/index` - All code related to the Elastic Search index and query service.

### Continuous Integration (Github Actions)

- **Testing**: (TODO) Go test suites on all pull requests
- **Quality Assurance**: (TODO) Automated linting, formatting, and security checks

### Continuous Deployment

- **Infrastructure as Code**: (TODO) [Terraform](https://developer.hashicorp.com/terraform/intro/use-cases) manages all cloud resources
- **Multi-Environment Support**: (TODO) Separate staging and production deployments with proper promotion workflows
- **Service Orchestration**: (TODO) Coordinated deployment of ingestion and indexing services in the correct dependency order
- **Multi-Cloud Management**:
  - **MS Azure**: Kubernetes clusters, cloud services, networking
  - **Google Cloud**: Temporary testing of kubernetes clusters, cloud services, networking
