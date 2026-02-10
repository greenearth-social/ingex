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
export GE_ELASTICSEARCH_SERVICE_USER_PWD="your-secure-password"

# Fresh deployment (first time)
./deploy.sh local --ctypes init

# Update index templates
./deploy.sh local --ctypes schema

# Update compute/storage resources
./deploy.sh local --ctypes resource
```

The deployment script handles all setup steps including Elasticsearch, Kibana, service user creation, and index template configuration.

## Deployment Guide

### Prerequisites

**All Environments:**
- kubectl
- `GE_ELASTICSEARCH_SERVICE_USER_PWD` environment variable set

**Local:**
- Docker and minikube (or other local Kubernetes cluster)

**Stage/Prod:**
- Google Cloud CLI (`gcloud`) installed and authenticated
- **Kubernetes Engine Admin** IAM role (for ECK operator installation)

### Deploy

Use the automated deployment script for all environments:

```bash
# Set required environment variable (use fc -p to avoid shell history)
export GE_ELASTICSEARCH_SERVICE_USER_PWD="your-secure-password"

# Deploy to any environment with change type
./deploy.sh local --ctypes init              # Fresh deployment
./deploy.sh local --ctypes schema            # Update templates
./deploy.sh local --ctypes resource          # Update resources
./deploy.sh local --ctypes schema,resource   # Update both

# Common options
./deploy.sh local --ctypes init --install-eck   # Install ECK operator first
./deploy.sh local --ctypes schema --dry-run     # Preview changes
./deploy.sh local --teardown                    # Delete environment
```

**Change Types (`--ctypes`)**:
- **`init`** - Fresh deployment (cannot be combined with other types)
- **`schema`** - Update index templates only
- **`resource`** - Update Elasticsearch compute/storage resources
- **`schema,resource`** - Update both (resources first, then schema)

The script:
- Creates/updates infrastructure using Kustomize
- Deploys Elasticsearch and Kibana via ECK operator
- Sets up authentication and bootstrap indices
- Tracks deployment state with git SHA checksums

### Configuration Structure

Kustomize base + overlay architecture:
- **`deploy/k8s/base/`** - Shared configuration for all environments
- **`deploy/k8s/environments/local/`** - Local overrides (2GB memory, 5GB storage)
- **`deploy/k8s/environments/stage/`** - Stage overrides (12GB memory, 20GB storage)

To customize an environment, edit the overlay in `deploy/k8s/environments/<env>/kustomization.yaml`.

## Graceful Updates

The deployment system supports graceful updates with zero or minimal downtime. You specify what to update using the `--ctypes` flag.

### Change Types

- **`init`** - Fresh deployment (first time setup)
- **`schema`** - Update index templates for non-breaking schema changes
- **`resource`** - Update Elasticsearch compute/storage resources via ECK rolling update
- **`schema,resource`** - Update both (resources first, then schema)

### Supported Schema Changes (Non-Breaking)

The deployment system supports the following **non-breaking** schema changes without reindexing:

✅ **Supported (Zero Downtime)**:
- **Adding new fields** to existing indices (indexed or non-indexed)
- **Adding dense_vector fields** for embeddings
- **Creating entirely new index types** (e.g., adding a `reposts` index)
- **Updating analyzers** (affects new documents only)

When you make these changes:
- Templates are updated via `PUT /_index_template` (idempotent operation)
- **New documents** ingested after the update will include the new fields
- **Existing documents** won't have the new fields (treated as `null` in queries - Elasticsearch handles this gracefully)
- No reindexing needed - fully backward compatible

❌ **Not Supported (Requires Reindexing - Out of Scope)**:
- **Changing field data types** (e.g., `text` → `keyword`)
- **Changing number of shards** (immutable after index creation)
- **Changing indexed fields** to non-indexed or vice versa
- **Major mapping restructuring**

These breaking changes would require blue-green deployment with reindexing, which is not implemented in the current system. If you need to make breaking changes, you'll need to manually create new indices and reindex data.

### Deployment Version Tracking

The system tracks deployment state in a ConfigMap called `elasticsearch-deployment-state`:

```bash
# Check current deployment version
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o yaml

# View deployment metadata
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o jsonpath='{.metadata.annotations}'

# Check git SHA (tracks manifest version at deployment time)
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o jsonpath='{.data.deployment-git-sha}'

# View last schema update timestamp
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o jsonpath='{.data.last-schema-update}'

# View last resource update timestamp
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o jsonpath='{.data.last-resource-update}'
```

The ConfigMap tracks:
- **last-schema-update**: Timestamp of last schema (template) update
- **last-resource-update**: Timestamp of last resource (CPU/memory) update
- **deployment-git-sha**: Git SHA of manifest at deployment time
- **index-types**: Comma-separated list of index types

### Deploying Non-Breaking Schema Changes

**Example**: Adding a `reply_count` field to the posts index

1. Edit the template file:
```bash
vim deploy/k8s/base/templates/posts-index-template.yaml
```

Add the new field to the `properties` section:
```yaml
reply_count:
  type: integer
  index: true
```

2. Deploy the change:
```bash
export GE_ELASTICSEARCH_SERVICE_USER_PWD="your-password"
./deploy.sh local --ctypes schema  # or stage, prod
```

The deployment script will:
- Update the `posts_template` via `PUT /_index_template/posts_template`
- Verify cluster health remains green/yellow
- Update deployment state ConfigMap with git SHA

3. Verify the template was updated:
```bash
kubectl port-forward svc/greenearth-es-http 9200 -n greenearth-local &
curl -k -u "elastic:PASSWORD" "https://localhost:9200/_index_template/posts_template"
```

4. Test with new documents:
```bash
# New documents can include the reply_count field
curl -k -X POST "https://localhost:9200/posts/_doc" \
  -u "es-service-user:PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{"content":"test post","reply_count":5}'

# Existing documents continue to work (reply_count will be null)
curl -k "https://localhost:9200/posts/_search?q=content:test"
```

### Deploying Resource Updates

**Example**: Increasing Elasticsearch memory from 2Gi to 4Gi

1. Edit the environment overlay:
```bash
vim deploy/k8s/environments/local/kustomization.yaml
```

Update the memory patch:
```yaml
- op: replace
  path: /spec/nodeSets/0/podTemplate/spec/containers/0/resources/requests/memory
  value: 4Gi
```

2. Deploy the change:
```bash
./deploy.sh local --ctypes resource
```

The deployment script will:
- Verify cluster health is green/yellow before proceeding
- Apply the updated Elasticsearch manifest
- ECK operator performs rolling update (one pod at a time)
- Wait for all pods to be updated and cluster to return to healthy state
- Update deployment state ConfigMap

3. Verify the update:
```bash
# Check pod resources
kubectl get pod elasticsearch-es-default-0 -n greenearth-local -o jsonpath='{.spec.containers[0].resources}'

# Check cluster health
kubectl get elasticsearch greenearth -n greenearth-local -o jsonpath='{.status.health}'
```

**Expected disruption**: ~30 seconds per node during rolling update. ECK maintains cluster quorum and redistributes shards gracefully.

### Rollback Procedures

#### Rolling Back Schema Changes (Non-Breaking)

Since non-breaking schema changes only affect future documents, rollback is straightforward:

```bash
# 1. Revert the template change in git
git revert <commit-hash>

# 2. Redeploy
./deploy.sh local --ctypes schema

# Result:
# - Template reverted to previous version
# - Future documents will use old schema
# - Existing documents are unaffected (they already have whatever fields they have)
```

**Note**: If your application is already using new fields, you may need to handle `null` values gracefully when querying older documents that don't have those fields.

#### Rolling Back Resource Changes

```bash
# 1. Revert the resource change in git
git revert <commit-hash>

# 2. Redeploy
./deploy.sh local --ctypes resource

# Result:
# - ECK operator performs rolling update back to previous spec
# - Cluster remains available during rollback
```

#### Emergency Rollback (Manual)

If the automated rollback doesn't work, you can manually intervene:

**For schema changes**:
```bash
# Get the previous template version from git or backup
git show HEAD~1:index/deploy/k8s/base/templates/posts-index-template.yaml > /tmp/old-template.yaml

# Manually update template
kubectl port-forward svc/greenearth-es-http 9200 -n greenearth-local &
curl -k -X PUT "https://localhost:9200/_index_template/posts_template" \
  -u "es-service-user:PASSWORD" \
  -H "Content-Type: application/json" \
  -d @/tmp/old-template.json
```

**For resource changes**:
```bash
# Edit Elasticsearch resource directly
kubectl edit elasticsearch greenearth -n greenearth-local

# Revert the spec changes manually
# Save and exit - ECK will roll back
```

### Best Practices for Production Deployments

1. **Always test in local first**: `./deploy.sh local --ctypes schema`
2. **Then test in stage**: `./deploy.sh stage --ctypes schema`
3. **Review deployment state** before and after:
   ```bash
   kubectl get configmap elasticsearch-deployment-state -n greenearth-stage -o yaml
   ```
4. **Use dry-run to preview changes**:
   ```bash
   ./deploy.sh prod --ctypes schema --dry-run
   ```
5. **Monitor cluster health during and after deployment**:
   ```bash
   watch kubectl get elasticsearch greenearth -n greenearth-prod -o jsonpath='{.status.health}'
   ```
6. **Check application metrics** for errors after schema changes
7. **Have rollback plan ready** before deploying to production

## Accessing the Cluster

### Access Kibana Web UI

```bash
# Port-forward to access Kibana (works for any environment)
kubectl port-forward service/greenearth-kb-http 5601 -n $GE_K8S_NAMESPACE
```

Browse to: **<https://localhost:5601>**

**Note**: You'll get a certificate warning (self-signed cert) - this is expected.

**Get the elastic superuser password:**

```bash
kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $GE_K8S_NAMESPACE
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
kubectl port-forward service/greenearth-es-http 9200 -n $GE_K8S_NAMESPACE
```

**Get credentials:**

```bash
# Elastic superuser (full access)
kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $GE_K8S_NAMESPACE

# Service user (limited to posts indices)
kubectl get secret es-service-user-secret -o go-template='{{.data.password | base64decode}}' -n $GE_K8S_NAMESPACE
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

The ingest and API services require separate API keys for authentication with different permission levels:

- **Ingest services** (jetstream, megastream, expiry): Need read/write access
- **API service**: Only needs read access

The `k8s_recreate_api_key.sh` script creates both keys automatically. For manual creation, follow these steps:

### 1. Create API Keys via Elasticsearch

With Elasticsearch running and accessible via port-forward:

```bash
# Get the elastic password first
ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $GE_K8S_NAMESPACE)

# Create the INGEST API key (read/write access)
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ingest-services",
    "role_descriptors": {
      "ingest_role": {
        "cluster": ["manage_index_templates", "monitor"],
        "indices": [
          {
            "names": ["posts", "posts_*", "post_tombstones", "post_tombstones_*", "likes", "likes_*", "like_tombstones", "like_tombstones_*", "hashtags", "hashtags*"],
            "privileges": ["all", "maintenance", "create_index", "auto_configure"]
          }
        ]
      }
    }
  }'

# Create the READONLY API key (read-only access for API service)
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api-services-readonly",
    "role_descriptors": {
      "readonly_role": {
        "cluster": ["monitor"],
        "indices": [
          {
            "names": ["posts", "posts_*", "post_tombstones", "post_tombstones_*", "likes", "likes_*", "like_tombstones", "like_tombstones_*", "hashtags", "hashtags*"],
            "privileges": ["read", "view_index_metadata"]
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
  "name": "ingest-services",
  "api_key": "VGhpcyBpcyBub3QgYSByZWFsIGtleQ==",
  "encoded": "YWJjMTIzOlRoaXMgaXMgbm90IGEgcmVhbCBrZXk="
}
```

### 2. Store API Keys in Google Secret Manager

Use the `encoded` values from the API key responses:

```bash
# Disable shell history
fc -p

# Store the INGEST API key (read/write - for ingest services)
echo -n "<INGEST_ENCODED_KEY>" | gcloud secrets create elasticsearch-api-key --data-file=-

# Store the READONLY API key (read-only - for API service)
echo -n "<READONLY_ENCODED_KEY>" | gcloud secrets create elasticsearch-api-key-readonly --data-file=-

# Also store the Elasticsearch URL for the services
echo -n "https://your-elasticsearch-cluster:9200" | gcloud secrets create elasticsearch-url --data-file=-
```

For production, use `-prod` suffix:
- `elasticsearch-api-key-prod` (ingest)
- `elasticsearch-api-key-readonly-prod` (API)

### 3. Deploy Ingest Services

See the docs at [/ingest/deploy/README.md](../ingest/deploy/README.md)

### API Key Management

- **Separation of concerns**: Ingest services have read/write keys, API has read-only key
- **Expiration**: Keys are set to expire after 365 days
- **Security**: Each key has minimal required permissions for its specific use case
- **Rotation**: Run `scripts/k8s_recreate_api_key.sh` to rotate both keys at once
- **Monitoring**: Check API key status via Kibana → Stack Management → Security → API Keys
- **Secrets**: 
  - `elasticsearch-api-key[-prod]` - Ingest services (read/write)
  - `elasticsearch-api-key-readonly[-prod]` - API service (read-only)

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
