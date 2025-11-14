# Elasticsearch Index Infrastructure

This directory contains all infrastructure and deployment configurations for the Elasticsearch indexing layer of the Green Earth Ingex system.

## Directory Structure

```text
index/
├── README.md                           # This file
├── deploy.sh                          # Automated deployment script
└── deploy/                            # Deployment configurations
    └── k8s/                          # Kubernetes manifests
        ├── base/                     # Shared Kustomize base configuration
        └── environments/
            ├── local/                # Local development environment
            └── stage/                # Stage environment (GKE)
```

## Infrastructure Overview

The indexing infrastructure uses **Elastic Cloud on Kubernetes (ECK)** to deploy and manage Elasticsearch clusters across different environments.

### Technology Stack

- **Elasticsearch 9.0.0**: Search engine and document store
- **Kibana 9.0.0**: Web UI for Elasticsearch management and visualization
- **ECK 3.1.0**: Kubernetes operator for Elasticsearch lifecycle management
- **Kubernetes**: Container orchestration (local: minikube, stage/prod: cloud)
- **Google Kubernetes Engine (GKE)**: Temporary cloud platform for stage testing (migrating to Azure)
- **Azure Kubernetes Service (AKS)**: Target production platform (future)

### Environment-Specific Configurations

#### Local Development

- **Elasticsearch**: Single-node cluster optimized for laptop resources
  - 2GB memory allocation with 1GB JVM heap
  - 5GB storage for testing
  - Security enabled (TLS with self-signed certificates)
  - Authentication required (native realm)
  - Resource requests: 2GB RAM, 500m CPU
- **Kibana**: Single instance web UI
  - 1GB memory allocation
  - No persistent storage needed
  - Security enabled (matching Elasticsearch)
  - Resource requests: 1GB RAM, 500m CPU

## Quick Start

Deploy to your environment with a single command:

```bash
# Start a private shell session
fc -p

# Set the service user password
export ES_SERVICE_USER_PASSWORD="your-secure-password"

# Deploy to local environment
./deploy.sh local

# Deploy to stage environment
./deploy.sh stage
```

The deployment script automatically handles all setup steps including Elasticsearch, Kibana, service user creation, and index template configuration.

## Deployment Guide

The deployment infrastructure uses **Kustomize** for configuration management with a shared base and environment-specific overlays.

### Prerequisites

**Local Environment:**

- Docker
- minikube or other local Kubernetes cluster
- kubectl
- ECK operator installed (or use `--install-eck` flag)

**Stage Environment:**

- Google Cloud CLI (`gcloud`) installed and authenticated
- **Kubernetes Engine Admin** IAM role for ECK operator installation
- kubectl installed locally
- ECK operator installed (or use `--install-eck` flag)

**Note**: GKE is temporary for initial testing. Future deployments will use Azure Kubernetes Service (AKS).

### Automated Deployment

The simplest way to deploy is using the automated deployment script:

```bash
# Set required environment variable
export ES_SERVICE_USER_PASSWORD="your-secure-password"

# Deploy to local environment
./deploy.sh local

# Deploy to stage with ECK installation
./deploy.sh stage --install-eck

# See all options
./deploy.sh --help
```

#### Deployment Script Options

- `--install-eck`: Install ECK operator before deployment
- `--skip-templates`: Skip template/alias ConfigMaps (for updates)
- `--dry-run`: Show what would be deployed without applying
- `--teardown`: Delete the entire environment (prompts for confirmation)

### Manual Deployment (Advanced)

If you prefer manual deployment or need to customize the process:

```bash
# 1. Set environment variables
export ENVIRONMENT=local  # or stage
export NAMESPACE=greenearth-$ENVIRONMENT
export ES_SERVICE_USER_PASSWORD="your-secure-password"

# 2. Create namespace
kubectl create namespace $NAMESPACE

# 3. For stage only: Deploy DaemonSet
kubectl apply -f deploy/k8s/environments/stage/max-map-count-daemonset.yaml

# 4. Deploy all resources using Kustomize
kubectl apply -k deploy/k8s/environments/$ENVIRONMENT

# 5. Wait for resources to be ready
kubectl get elasticsearch,kibana -n $NAMESPACE -w

# 6. Create service user secret
kubectl create secret generic es-service-user-secret \
  --from-literal=username="es-service-user" \
  --from-literal=password="$ES_SERVICE_USER_PASSWORD" \
  -n $NAMESPACE

# 7. Deploy and wait for service user setup job
kubectl apply -f deploy/k8s/base/es-service-user-setup-job.yaml -n $NAMESPACE
kubectl wait --for=condition=complete --timeout=180s job/es-service-user-setup -n $NAMESPACE

# 8. Deploy and wait for bootstrap job
kubectl apply -f deploy/k8s/base/bootstrap-job.yaml -n $NAMESPACE
kubectl wait --for=condition=complete --timeout=180s job/elasticsearch-bootstrap -n $NAMESPACE
```

### Kustomize Configuration

The deployment uses Kustomize with a base + overlay structure:

- **base/**: Shared configuration for all environments
  - Elasticsearch, Kibana, bootstrap job, service user setup
  - Index templates and aliases (shared across environments)
- **environments/local/**: Local-specific overrides
  - 2GB memory, mmap disabled, 5GB storage
- **environments/stage/**: Stage-specific overrides
  - 12GB memory, mmap enabled, 20GB storage, DaemonSet for vm.max_map_count

This structure eliminates configuration duplication and makes it easy to add new environments.

## Accessing the Cluster

### Access Kibana Web UI

```bash
# Port-forward to access Kibana (works for any environment)
kubectl port-forward service/greenearth-kb-http 5601 -n $NAMESPACE
```

Browse to: **<https://localhost:5601>**

**Note**: You'll get a certificate warning (self-signed cert) - this is expected.

**Get the elastic superuser password:**

```bash
kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE
```

**Login with:**

- **Username**: `elastic`
- **Password**: (from command above)

Kibana provides:

- **Dev Tools Console**: Interactive API testing at `/app/dev_tools#/console`
- **Index Management**: View and manage indices at `/app/management/data/index_management`
- **Stack Management**: Configure settings at `/app/management`
- **Discover**: Explore your data at `/app/discover`

### Access Elasticsearch API

**Port-forward Elasticsearch:**

```bash
kubectl port-forward service/greenearth-es-http 9200 -n $NAMESPACE
```

**Get credentials:**

```bash
# Elastic superuser (full access)
kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE

# Service user (limited to posts indices)
kubectl get secret es-service-user-secret -o go-template='{{.data.password | base64decode}}' -n $NAMESPACE
```

**Test API:**

```bash
# Using elastic user
curl -k -u "elastic:PASSWORD" https://localhost:9200/

# Check cluster health
curl -k -u "elastic:PASSWORD" https://localhost:9200/_cluster/health

# Using service user
curl -k -u "es-service-user:PASSWORD" https://localhost:9200/_cluster/health

# Verify index templates and aliases
curl -k -u "es-service-user:PASSWORD" https://localhost:9200/_index_template/posts_template
curl -k -u "es-service-user:PASSWORD" https://localhost:9200/_alias/posts
```

## Generating API Keys for Ingest Services

The ingest services (see `../ingest/README.md`) require API keys for authentication. Follow these steps to create and configure them:

### 1. Create API Key via Elasticsearch

With Elasticsearch running and accessible via port-forward:

```bash
# Get the elastic password first
ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE)

# Create the API key (no expiration for operational simplicity)
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ingest-service-key",
    "role_descriptors": {
      "ingest_role": {
        "cluster": ["manage_index_templates", "monitor"],
        "indices": [
          {
            "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1", "likes", "likes_v1"],
            "privileges": ["create_doc", "create", "delete", "index", "write", "all"]
          }
        ]
      }
    }
  }'
```

**Expected Response:**

```json
{
  "id": "abc123...",
  "name": "ingest-service-key",
  "api_key": "VGhpcyBpcyBub3QgYSByZWFsIGtleQ==",
  "encoded": "YWJjMTIzOlRoaXMgaXMgbm90IGEgcmVhbCBrZXk="
}
```

### 2. Store API Key in Google Secret Manager

Use the `encoded` value from the API key response:

```bash
# Disable shell history
fc -p

# Store the encoded API key (replace with actual value from response)
echo -n "YWJjMTIzOlRoaXMgaXMgbm90IGEgcmVhbCBrZXk=" | gcloud secrets create elasticsearch-api-key --data-file=-

# Also store the Elasticsearch URL for the ingest services
echo -n "https://your-elasticsearch-cluster:9200" | gcloud secrets create elasticsearch-url --data-file=-
```

### 3. Deploy Ingest Services

Now you can deploy the ingest services which will use these secrets:

```bash
cd ../ingest
export PROJECT_ID="your-gcp-project"
export S3_BUCKET="your-megastream-bucket"
export S3_PREFIX="megastream/databases/"

./setup.sh
./deploy.sh
```

### API Key Management

- **Expiration**: Keys are set to never expire for operational simplicity
- **Security**: Keys have minimal required permissions for ingest operations only
- **Rotation**: Manual rotation can be done by creating new keys and updating Secret Manager
- **Monitoring**: Check API key status via Kibana → Stack Management → Security → API Keys
- **Future**: Add automated key rotation and expiration when building more advanced key management

**Expected responses:**

- **Basic connectivity**: Elasticsearch version info and tagline
- **Cluster health**: `status: "green"`, `number_of_nodes: 1`
- **Index template**: Shows posts_template configuration with schema
- **Alias**: Shows `posts` alias pointing to `posts_v1` index

## Cleanup

Using the deployment script (recommended):

```bash
# Teardown with confirmation prompt
./deploy.sh local --teardown
./deploy.sh stage --teardown
```

Or manually:

```bash
# Remove all resources
kubectl delete namespace greenearth-local  # or greenearth-stage
```
