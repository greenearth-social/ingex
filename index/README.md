# Elasticsearch Index Infrastructure

This directory contains all infrastructure and deployment configurations for the Elasticsearch indexing layer of the Green Earth Ingex system.

## Directory Structure

```text
index/
├── README.md                           # This file
├── deploy.sh                          # Automated deployment script
└── deploy/                            # Deployment configurations
    ├── terraform/                     # Terraform IaC (TODO)
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

#### Production

- **Multi-node cluster** (1 master + 2 data nodes) - TODO: upgrade cluster sizes depending on scale
- **Higher resource allocation** for production workloads
- **50GB storage per data node**
- **TODO**: Proper virtual memory configuration via init containers

## Quick Start

Deploy to your environment with a single command:

```bash
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

#### What the Script Does

The deployment script automatically:

1. Verifies prerequisites (kubectl, cluster connection, environment variables)
2. Optionally installs ECK operator (with `--install-eck`)
3. Creates the namespace
4. Deploys stage-specific DaemonSet (for stage environment only)
5. Applies all Kustomize manifests (Elasticsearch, Kibana, templates)
6. Waits for Elasticsearch and Kibana to be ready
7. Creates service user credentials secret
8. Runs service user setup job
9. Runs bootstrap job to configure templates and aliases
10. Provides next steps for accessing the cluster

The script includes automatic cleanup on failure and colored output for easy monitoring.

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

# 7. Deploy service user setup job
kubectl apply -f deploy/k8s/environments/$ENVIRONMENT/es-service-user-setup-job-patch.yaml -n $NAMESPACE

# 8. Deploy bootstrap job
kubectl apply -f deploy/k8s/environments/$ENVIRONMENT/bootstrap-job-patch.yaml -n $NAMESPACE
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

**Local:**

```bash
# Port-forward to access Kibana
kubectl port-forward service/greenearth-kibana-local-kb-http 5601 -n $NAMESPACE
```

Browse to: **<https://localhost:5601>**

**Stage:**

```bash
# Port-forward to access Kibana
kubectl port-forward service/greenearth-kibana-stage-kb-http 5601 -n $NAMESPACE
```

Browse to: **<https://localhost:5601>**

**Note**: You'll get a certificate warning (self-signed cert) - this is expected.

**Get the elastic superuser password:**

Local:

```bash
kubectl get secret greenearth-es-local-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE
```

Stage:

```bash
kubectl get secret greenearth-es-stage-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE
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

**Port-forward Elasticsearch** (replace `NAMESPACE`):

Local:

```bash
kubectl port-forward service/greenearth-es-local-es-http 9200 -n $NAMESPACE
```

Stage:

```bash
kubectl port-forward service/greenearth-es-stage-es-http 9200 -n $NAMESPACE
```

**Get credentials:**

Local:

```bash
# Elastic superuser (full access)
kubectl get secret greenearth-es-local-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE

# Service user (limited to posts indices)
kubectl get secret es-service-user-secret -o go-template='{{.data.password | base64decode}}' -n $NAMESPACE
```

Stage:

```bash
# Elastic superuser (full access)
kubectl get secret greenearth-es-stage-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $NAMESPACE

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

The ingest service (see ../ingest/README.md) will need an ES API key which can be generated like so:

```sh
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ingest-service-key",
    "expiration": "90d",
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

**Expected responses:**

- **Basic connectivity**: Elasticsearch version info and tagline
- **Cluster health**: `status: "green"`, `number_of_nodes: 1`
- **Index template**: Shows posts_template configuration with schema
- **Alias**: Shows `posts` alias pointing to `posts_v1` index

### Health Check Verification

A healthy deployment should show:

- ✅ Elasticsearch cluster status: `green`
- ✅ Elasticsearch nodes: `1`
- ✅ Kibana status: `green`
- ✅ Kibana accessible at <https://localhost:5601>
- ✅ Service user setup job completed: `1/1`
- ✅ Bootstrap job completed: `1/1`
- ✅ Posts index template applied
- ✅ Posts alias configured: `posts` → `posts_v1`
- ✅ API responding with version `9.0.0`

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

## Azure Deployment (Future)

The final production environment will be deployed on Azure Kubernetes Service (AKS). This section will be populated once the GKE stage environment is validated and Azure infrastructure is set up.

**Planned Changes:**

- Migrate from GKE Autopilot to AKS
- Update deployment scripts for Azure CLI
- Configure Azure-specific networking and security
- Adapt DaemonSet/init containers for AKS node configuration

## Production Deployment

(TODO - will be copied from stage/Azure once validated)

## Troubleshooting

### Common Issues

#### Pod in CrashLoopBackOff

- Check logs: `kubectl logs POD_NAME -n $NAMESPACE`
- Common causes: Memory limits, configuration conflicts

#### OOMKilled Errors

- Reduce JVM heap size in manifest
- Increase memory limits if resources allow

#### Configuration Conflicts

- Avoid mixing `discovery.type: single-node` with ECK auto-configuration
- Let ECK handle single-node setup automatically

#### Port-forward Issues

- Ensure service exists: `kubectl get svc -n $NAMESPACE`
- Check if port 9200 is already in use locally
