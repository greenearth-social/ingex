# Green Earth Ingex - Cloud Run Deployment

This directory contains configuration and deployment scripts for running the Green Earth Ingex services on Google Cloud Platform using Cloud Run.

## Overview

The deployment consists of three main components:

1. **jetstream-ingest** - Long-running Cloud Run service that connects to BlueSky Jetstream WebSocket API and ingests "Like" events
2. **megastream-ingest** - Long-running Cloud Run service that processes BlueSky content from SQLite databases stored in S3
3. **elasticsearch-expiry** - Cloud Run job that runs daily to clean up old data from Elasticsearch

## Quick Start

### 1. First-Time Setup

Run the setup script to configure your GCP environment:

```bash
# disable history
fc -p

cd ingest/

# Set your configuration
export PROJECT_ID="your-gcp-project"
export ELASTICSEARCH_URL="https://your-elasticsearch-cluster:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
export S3_BUCKET="your-megastream-bucket"
export S3_PREFIX="megastream/databases/"

# Run setup
./gcp_setup.sh
```

This will:
- Enable required GCP APIs
- Create Artifact Registry for container images
- Set up service accounts with proper permissions
- Create secrets in Secret Manager
- Configure Cloud Scheduler for daily data expiry

### 2. Deploy Services

Deploy all services to Cloud Run:

```bash
./deploy.sh
```

This will:
- Build container images using Cloud Build
- Deploy jetstream-ingest and megastream-ingest as services
- Deploy elasticsearch-expiry as a scheduled job
- Show status and service URLs

## Configuration

### Environment Variables

Set these before running the scripts:

```bash
# Required
export PROJECT_ID="your-gcp-project"
export ELASTICSEARCH_URL="https://your-elasticsearch-cluster:9200"
export ELASTICSEARCH_API_KEY="your-api-key"
export S3_BUCKET="your-megastream-bucket"
export S3_PREFIX="megastream/databases/"

# Optional
export REGION="us-central1"              # GCP region
export ENVIRONMENT="dev"                 # Environment suffix
```

### Command Line Options

Both scripts support command line options:

```bash
# Setup script
./gcp_setup.sh --project-id PROJECT --elasticsearch-url URL --elasticsearch-api-key KEY

# Deploy script
./gcp_deploy.sh --project-id PROJECT --region REGION --skip-build
```

Use `--help` for full option lists.

## Service Architecture

### Jetstream Ingest Service

- **Type**: Cloud Run Service (long-running)
- **Purpose**: Ingests BlueSky "Like" events from Jetstream WebSocket API
- **Scaling**: 1 instance (WebSocket connection requires persistent state)
- **Resources**: 1 CPU, 512Mi memory
- **State**: Cursor tracking stored in `/tmp/jetstream_state.json`

### Megastream Ingest Service

- **Type**: Cloud Run Service (long-running)
- **Purpose**: Processes BlueSky posts from S3-stored SQLite databases
- **Scaling**: 1 instance (file processing with state tracking)
- **Resources**: 1 CPU, 1Gi memory
- **State**: File processing cursor in `/tmp/megastream_state.json`
- **Mode**: Continuous polling every 5 minutes

### Elasticsearch Expiry Job

- **Type**: Cloud Run Job (scheduled)
- **Purpose**: Daily cleanup of old documents from Elasticsearch
- **Schedule**: Daily at 2 AM UTC (via Cloud Scheduler)
- **Resources**: 1 CPU, 512Mi memory
- **Retention**: 60 days by default

## Secrets Management

Sensitive configuration is stored in Google Secret Manager:

| Secret Name | Description |
|-------------|-------------|
| `elasticsearch-url` | Elasticsearch cluster endpoint |
| `elasticsearch-api-key` | Elasticsearch API key with read/write permissions |
| `s3-bucket` | S3 bucket name containing SQLite files |
| `s3-prefix` | S3 key prefix for SQLite files |

## Monitoring and Logs

### View Service Logs

```bash
# Jetstream ingest logs
gcloud run services logs read jetstream-ingest --region=us-central1

# Megastream ingest logs
gcloud run services logs read megastream-ingest --region=us-central1

# Expiry job logs (latest execution)
gcloud run jobs logs read elasticsearch-expiry --region=us-central1
```

### Service Status

```bash
# List all services
gcloud run services list --region=us-central1

# Get service details
gcloud run services describe jetstream-ingest --region=us-central1
```

### Manual Operations

```bash
# Manually execute expiry job
gcloud run jobs execute elasticsearch-expiry --region=us-central1

# Update service configuration
gcloud run services update jetstream-ingest --region=us-central1 --cpu=2
```

## Development Workflow

### Local Testing

All services support dry-run mode for testing:

```bash
# Test locally with dry-run
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_API_KEY="test-key"

go run cmd/jetstream_ingest/main.go --dry-run --skip-tls-verify
go run cmd/megastream_ingest/main.go --source=local --mode=once --dry-run
go run cmd/elasticsearch_expiry/main.go --dry-run --retention-days=30
```

### Deployment Options

```bash
# Deploy only specific changes
./deploy.sh --skip-build  # Use existing images

# Deploy with different scaling
./deploy.sh --jetstream-instances 2 --megastream-instances 3

# Deploy to different environment
export ENVIRONMENT="staging"
./gcp_setup.sh && ./deploy.sh
```

## Troubleshooting

### Common Issues

**Build failures:**
- Ensure Docker is installed or use Cloud Build
- Check Artifact Registry permissions
- Verify project APIs are enabled

**Permission errors:**
- Confirm service account has proper IAM roles
- Check Secret Manager access permissions
- Verify Elasticsearch API key permissions

**Service startup issues:**
- Check Cloud Run logs for error messages
- Verify all required secrets are accessible
- Confirm Elasticsearch cluster is reachable

**Data ingestion problems:**
- Monitor service logs for processing errors
- Check Elasticsearch cluster health and capacity
- Verify S3 bucket access and file availability

### Debug Commands

```bash
# Check service account permissions
gcloud projects get-iam-policy PROJECT_ID --flatten="bindings[].members" --filter="bindings.members:ingex-runner*"

# Test secret access
gcloud secrets versions access latest --secret=elasticsearch-url

# Check Cloud Scheduler status
gcloud scheduler jobs list --location=us-central1

# View detailed service configuration
gcloud run services describe SERVICE_NAME --region=us-central1 --format=yaml
```

## Production Considerations

### Security
- Use least-privilege service accounts
- Rotate API keys regularly
- Enable VPC connector for private Elasticsearch access
- Use Google Secret Manager for all sensitive data

### Reliability
- Monitor service health and logs
- Set up alerting for failures
- Consider multi-region deployment for high availability
- Implement proper error handling and retries

### Performance
- Monitor resource usage and adjust CPU/memory as needed
- Optimize batch sizes for Elasticsearch operations
- Consider scaling limits based on data volume
- Use appropriate instance counts for your workload

### Cost Optimization
- Use minimum instances settings appropriate for your load
- Monitor Cloud Run costs and optimize resource allocation
- Consider using Cloud Scheduler's retry policies efficiently
- Right-size instance resources based on actual usage

## Files Structure

```
ingest/
├── setup.sh                    # One-time environment setup
├── deploy.sh                   # Main deployment script
├── deploy/
│   └── cloud-run/
│       ├── jetstream-ingest.yaml           # Service configuration
│       ├── megastream-ingest.yaml          # Service configuration
│       ├── elasticsearch-expiry-job.yaml   # Job configuration
│       ├── cloudbuild-*.yaml              # Build configurations
│       └── Dockerfile.*                   # Container definitions
└── README.md                   # This file
```

Generated during deployment:
- `cloudbuild-*.yaml` - Cloud Build configurations
- `Dockerfile.*` - Container build files
