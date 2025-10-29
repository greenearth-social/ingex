# Elasticsearch Index Infrastructure

This directory contains all infrastructure and deployment configurations for the Elasticsearch indexing layer of the Green Earth Ingex system.

## Directory Structure

```
index/
├── README.md                           # This file
└── deploy/                            # Deployment configurations
    ├── terraform/                     # Terraform IaC (TODO)
    └── k8s/                          # Kubernetes manifests
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

## Deployment Guide

This guide covers deploying Elasticsearch and Kibana to both local (minikube) and stage (GKE Autopilot) environments. The deployment steps are nearly identical between environments, with specific differences noted where applicable.

### Prerequisites

**Local Environment:**
- Docker
- minikube or other local Kubernetes cluster
- kubectl
- openssl (for generating passwords)

**Stage Environment:**
- Google Cloud CLI (`gcloud`) installed and authenticated
- **Kubernetes Engine Admin** IAM role for ECK operator installation
- kubectl installed locally
- openssl (for generating passwords)

**Note**: GKE is temporary for initial testing. Future deployments will use Azure Kubernetes Service (AKS).

### 1. Create or Configure Kubernetes Cluster

**Local:**
```bash
# Ensure minikube is running
minikube status
```

**Stage:**
```bash
# Create GKE Autopilot cluster
gcloud container clusters create-auto greenearth-stage-cluster \
  --region=us-east1 \
  --project=YOUR_PROJECT_ID

# Verify connection
kubectl config current-context
# Should show: gke_PROJECT_ID_us-east1_greenearth-stage-cluster
```

### 2. Install ECK Operator

**Both Environments:**
```bash
# Install ECK 3.1.0 CRDs
kubectl create -f https://download.elastic.co/downloads/eck/3.1.0/crds.yaml

# Install ECK Operator
kubectl apply -f https://download.elastic.co/downloads/eck/3.1.0/operator.yaml

# Verify ECK is running
kubectl get pods -n elastic-system
# Wait for elastic-operator to show STATUS: Running
```

### 3. Create Namespace

**Local:**
```bash
kubectl create namespace greenearth-local
```

**Stage:**
```bash
kubectl create namespace greenearth-stage
```

**IMPORTANT**: From here on, greenearth-local / greenearth-stage will be referred to as $NAMESPACE. 
The environment, local/stage, will be referred to as $DEPLOYMENT_ENV.

### 4. Deploy DaemonSet for Virtual Memory (Stage Only)

**Stage Only** - **IMPORTANT**: Must be deployed before Elasticsearch to set `vm.max_map_count`.

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/max-map-count-daemonset.yaml
```

Wait ~30 seconds for DaemonSet to complete.

**Note**: Local environments using minikube don't typically require this step.

### 5. Deploy Elasticsearch

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/elasticsearch.yaml
```

**Configuration Differences:**
- **Local**: 2GB memory, 1GB JVM heap, 5GB storage, mmap disabled
- **Stage**: 12GB memory, 6GB JVM heap, 20GB storage, mmap enabled

### 6. Deploy Kibana

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/kibana.yaml
```

### 7. Wait for Elasticsearch and Kibana to be Ready

```bash
# Check Elasticsearch status
kubectl get elasticsearch -n $NAMESPACE

# Check Kibana status
kubectl get kibana -n $NAMESPACE

# Check pod status
kubectl get pods -n $NAMESPACE
```

Wait for:
- **Elasticsearch HEALTH**: `green`
- **Elasticsearch PHASE**: `Ready`
- **Kibana HEALTH**: `green`
- **Kibana PHASE**: `Ready`

**Timing:**
- Local: ~2-3 minutes
- Stage: ~5-10 minutes (first deployment)

### 8. Deploy ConfigMaps for Index Templates

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/templates/
```

### 9. Create Service User Credentials Secret

**IMPORTANT**: This secret must be created before running the service user setup job.

```bash
# Create the Kubernetes secret with both username and password (password should come from .env file)
kubectl create secret generic es-service-user-secret \
  --from-literal=username="es-service-user" \
  --from-literal=password="$ES_SERVICE_PASSWORD" \
  -n $NAMESPACE
```

### 10. Create Service Account User

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/es-service-user-setup-job.yaml
```

This job:
- Waits for Elasticsearch to be ready
- Creates `es_service_role` with index template and posts index permissions
- Creates the service user (using credentials from `es-service-user-secret`) with the role

**Monitor the job**:
```bash
kubectl get jobs -n $NAMESPACE
kubectl logs -l job-name=es-service-user-setup -n $NAMESPACE
```

### 11. Run Bootstrap Job

```bash
kubectl apply -f deploy/k8s/environments/$DEPLOYMENT_ENV/bootstrap-job.yaml
```

This job uses the `es-service-user` credentials to:
- Apply index templates
- Create initial indexes
- Configure initial index aliases

**Monitor the job**:
```bash
kubectl get jobs -n $NAMESPACE
kubectl logs -l job-name=elasticsearch-bootstrap -n $NAMESPACE
```

## Accessing the Cluster

### Access Kibana Web UI

**Local:**
```bash
# Port-forward to access Kibana
kubectl port-forward service/greenearth-kibana-local-kb-http 5601 -n $NAMESPACE
```

Browse to: **https://localhost:5601**

**Stage:**
```bash
# Port-forward to access Kibana
kubectl port-forward service/greenearth-kibana-stage-kb-http 5601 -n $NAMESPACE
```

Browse to: **https://localhost:5601**

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
            "names": ["posts", "posts_v1", "post_tombstones", "post_tombstones_v1"],
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
- ✅ Kibana accessible at https://localhost:5601
- ✅ Service user setup job completed: `1/1`
- ✅ Bootstrap job completed: `1/1`
- ✅ Posts index template applied
- ✅ Posts alias configured: `posts` → `posts_v1`
- ✅ API responding with version `9.0.0`

## Cleanup

```bash
# Remove all resources
kubectl delete namespace $NAMESPACE
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

**Pod in CrashLoopBackOff**
- Check logs: `kubectl logs POD_NAME -n $NAMESPACE`
- Common causes: Memory limits, configuration conflicts

**OOMKilled Errors**
- Reduce JVM heap size in manifest
- Increase memory limits if resources allow

**Configuration Conflicts**
- Avoid mixing `discovery.type: single-node` with ECK auto-configuration
- Let ECK handle single-node setup automatically

**Port-forward Issues**
- Ensure service exists: `kubectl get svc -n $NAMESPACE`
- Check if port 9200 is already in use locally