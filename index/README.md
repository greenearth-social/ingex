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
./deploy.sh local --ctypes snapshot          # Update SLM snapshot policy
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
- **`snapshot`** - Update SLM snapshot policy (schedule/retention) on a live cluster
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
- **`snapshot`** - Update the SLM snapshot policy (schedule/retention) on a live cluster
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

# View deployment history (rollback targets, newest first)
kubectl get configmap elasticsearch-deployment-state -n greenearth-local -o jsonpath='{.data.deployment-history}'
```

The ConfigMap tracks:
- **last-schema-update**: Timestamp of last schema (template) update
- **last-resource-update**: Timestamp of last resource (CPU/memory) update
- **deployment-git-sha**: Git SHA of manifest at deployment time
- **deployment-history**: The last 20 deployments as `<timestamp> <git-sha> <ctypes>`,
  newest first. `deployment-git-sha` says what is deployed now; this says what came
  before it, which is what [rolling back](#rollback-procedures) needs.
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

Elasticsearch does not roll back the way the Cloud Run services do. There is no
image to swap: the cluster is described by manifests that the ECK operator
reconciles, so rolling back means re-applying an earlier revision's manifests
and letting the operator converge on them.

`rollback.sh` does that without hand-managing a detached checkout mid-incident.
It reads the previous deployment's git sha from the cluster's own state
ConfigMap, materializes that revision's manifests in a temporary git worktree
(your working tree is never touched), and applies them through the *current*
`deploy.sh` via `--manifests-dir` — so the rollback runs on reviewed deploy
logic rather than re-executing whatever `deploy.sh` looked like at that sha.

```bash
./rollback.sh stage --list                    # deployment history, newest first
./rollback.sh stage                           # undo the last deployment
./rollback.sh prod --to a1b2c3d --ctypes schema
```

`--dry-run` shows the manifests that would be applied and changes nothing;
`--yes` skips the confirmation prompt. With no `--to`, the target is the newest
history entry whose sha differs from what is deployed, and the change types
default to the ones the deployment being undone applied.

#### Finding a rollback target

`deploy.sh` records every deployment in the `elasticsearch-deployment-state`
ConfigMap as `deployment-history` — newline-separated `<timestamp> <git-sha>
<ctypes>` entries, newest first, capped at 20. The older `deployment-git-sha`
key holds only what is deployed *now*, which is not enough to roll back from.

```bash
kubectl get configmap elasticsearch-deployment-state -n greenearth-prod \
  -o jsonpath='{.data.deployment-history}'
```

History accumulates from the change that introduced it onward. For deployments
made before that, pick a target from `git log --oneline -- deploy/k8s`.

#### What a rollback does and does not undo

| Change | Rolls back? | Notes |
| --- | --- | --- |
| Index templates / ILM (`--ctypes schema`) | Future indices only | Templates apply at index creation. Documents already written keep the mapping they were written with. |
| Resources / topology (`--ctypes resource`) | Yes | ECK performs a rolling update back to the previous spec; the cluster stays available. |
| Snapshot policy (`--ctypes snapshot`) | Yes | SLM schedule and retention are just configuration. |
| Elasticsearch version | **No** | See below. |
| Indexed data | No | See below. |

**Version downgrades are not possible.** Elasticsearch does not support
in-place downgrades — a cluster that has started on a newer version cannot be
moved back. `rollback.sh` compares `spec.version` in the target manifests
against what is deployed and refuses rather than letting ECK fail halfway. The
only way back to an older version is restoring a snapshot into a cluster running
that version:

```bash
./restore.sh --environment prod --snapshot <name>
```

**Data is not rolled back.** A manifest rollback changes configuration, not
documents. A breaking mapping change that has already been written to needs a
reindex (`tools/reindex.py`, see the [ingex README](../README.md#index-schema-migrations))
or a snapshot restore.

#### Emergency rollback (manual)

If `rollback.sh` cannot run — no history, a revision predating the current
manifest layout, or a broken toolchain — intervene directly:

**Resource/topology changes**: edit the CR and let ECK reconcile.

```bash
kubectl edit elasticsearch greenearth -n greenearth-prod
```

**Schema changes**: push the old template straight at the cluster.

```bash
git show <sha>:index/deploy/k8s/base/templates/posts-index-template.yaml > /tmp/old-template.yaml

kubectl port-forward svc/greenearth-es-http 9200 -n greenearth-prod &
curl -k -X PUT "https://localhost:9200/_index_template/posts_template" \
  -u "es-service-user:PASSWORD" \
  -H "Content-Type: application/json" \
  -d @/tmp/old-template.json
```

Note that `kubectl rollout undo` is the wrong tool here: the ES StatefulSets are
created and reconciled by the ECK operator, which will simply put back whatever
the `Elasticsearch` CR says. Roll back through the CR or the manifests, never
the StatefulSet.

Ingest and API services roll back separately and independently — see
[ingest/README.md](../ingest/README.md) and the api repo.

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

## Inferences Index

The `inferences` index stores per-post inference data extracted from Megastream SQLite files (sentiment, toxicity, moderation, topic, embeddings, etc.). This data is intended for offline analysis via parquet exports.

**Fields:**
- `at_uri` (keyword, indexed): AT-URI of the post
- `inferences` (object, `enabled: false`): Raw inference JSON stored without field mapping explosion
- `indexed_at` (date): When the document was indexed (used for ILM and time-range filtering)

**Lifecycle:** The `inferences_ilm_policy` ILM policy uses a hot+delete rollover pattern. The hot phase rolls over when the index reaches `INFERENCE_MAX_AGE` (stage: `1h`, prod: `1d`). After rollover the old index moves to the delete phase and is removed. ILM creates successive concrete indices (`inferences-000001`, `inferences-000002`, …) and keeps the `inferences` write alias pointing to the current write index throughout.

**Bootstrap:** The bootstrap job creates the `inferences_ilm_policy` ILM policy first, then applies the index template, and finally creates the concrete index `inferences-000001` with the `inferences` alias set as `is_write_index: true`. This ensures ILM can manage rollovers without losing the alias. The bootstrap job runs as `es-service-user`, which requires the `manage_ilm` cluster privilege to create and manage ILM policies — this is granted via `es_service_role` in `es-service-user-setup-job.yaml`.

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
            "names": ["posts*", "likes*",
              "post_tombstones", "post_tombstones_*", "post-tombstones-*",
              "like_tombstones", "like_tombstones_*", "like-tombstones-*",
              "hashtags", "hashtags*", "inferences", "inferences-*"],
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
            "names": ["posts*", "likes*",
              "post_tombstones", "post_tombstones_*", "post-tombstones-*",
              "like_tombstones", "like_tombstones_*", "like-tombstones-*",
              "hashtags", "hashtags*", "inferences", "inferences-*"],
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

## Backups & Restore

Snapshots are taken via Elasticsearch SLM and stored in GCS. Schedule and retention are configurable per environment via the `snapshot-settings` ConfigMap:

| Environment | Schedule | Retention |
|-------------|----------|-----------|
| stage | Hourly | 3 hours / max 3 snapshots |
| prod | Daily at 9am UTC (4am ET) | 14 days / max 14 snapshots |

### One-time GCP setup (per environment)

Run once to create the GCS bucket and Workload Identity bindings:

```bash
source .env.stage && ./index/gcp_setup.sh
source .env.prod && ./index/gcp_setup.sh
```

This creates:

- GCS bucket: `greenearth-471522-es-snapshots-<env>`
- GCP SA: `es-snapshot-<env>@greenearth-471522.iam.gserviceaccount.com`
- Workload Identity binding for `greenearth-<env>/es-node-sa`

### Create/update snapshot schedule or retention

After changing `snapshot_schedule`, `snapshot_expire_after`, or `snapshot_max_count` in a kustomization overlay, apply the changes to a live cluster with:

```bash
./deploy.sh stage --ctypes snapshot
./deploy.sh prod  --ctypes snapshot
```

This updates the `snapshot-settings` ConfigMap and re-runs the snapshot setup job to apply the new SLM policy to Elasticsearch. It also updates the ES service
user with necessary permissions for snapshot management.

### Verifying snapshots

```bash
# Get elastic password
ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $GE_K8S_NAMESPACE)

# Port-forward first
kubectl port-forward service/greenearth-es-http 9200 -n $GE_K8S_NAMESPACE

# Confirm snapshot repo is green
curl -k -u "elastic:$ELASTIC_PASSWORD" https://localhost:9200/_snapshot/gcs_backup

# Confirm SLM policy and next execution time
curl -k -u "elastic:$ELASTIC_PASSWORD" https://localhost:9200/_slm/policy/daily-snapshots

# Manually trigger a snapshot
curl -k -u "elastic:$ELASTIC_PASSWORD" -X PUT https://localhost:9200/_slm/policy/daily-snapshots/_execute

# List available snapshots
curl -k -u "elastic:$ELASTIC_PASSWORD" https://localhost:9200/_snapshot/gcs_backup/_all?verbose=false
```

### Restoring from a snapshot

#### Pre-Conditions

Data ingestion, exports, and deletion should be paused:

```bash
# Stop all services/jobs writing to ES.
./ingest/scripts/ingestctl.sh stop
```

The ES cluster must be healthy and accepting requests. If the cluster is
healthy and accepting requests, you can skip to the restore step.

If the cluster is not accepting requests, 
it may be faster to destroy and rebuild the entire cluster:

```bash
# Destroy the namespace then rebuild from scratch (warning: desctructive!)
./index/deploy.sh <env> --teardown
./index/deploy.sh <env> --ctypes init
```

#### Restoring

Run the restore script:

```bash
# Restore latest successful snapshot
./index/restore.sh --environment stage

# Restore a specific snapshot
./index/restore.sh --environment prod --snapshot daily-snap-2026.03.15

# Preview without executing
./index/restore.sh --environment stage --dry-run
```

Check progress of the data recovery:

```bash
# List all shards actively recovering
curl -k -u "elastic:$ELASTIC_PASSWORD" \
    "https://localhost:9200/_cat/recovery?active_only=true&v&h=index,shard,stage,files_percent,bytes_percent,time"
```

Restart the ingestion services:

```bash
cd ingest && ./scripts/ingestctl.sh start
```

**Note:** If the cluster was destroyed and rebuilt, you'll need to recreate API keys for the
ES cluster, then redeploy ingestion services and the API server to pick up the new keys.

```bash
# Recreate API keys on ES cluster:
cd ingest && ./scripts/k8s_recreate_api_keys.sh
# Redeploy ingestion services
./scripts/deploy.sh
# Restart the scheduled jobs
./scripts/ingestctl.sh start expiry
./scripts/ingestctl.sh start extract
# Redeploy API services
cd ../../api/ && source .env.example && ./scripts/deploy.sh
```

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
