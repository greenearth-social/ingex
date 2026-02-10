#!/bin/bash
# Recreate Elasticsearch API keys for Cloud Run services
# Creates two API keys:
#   1. Ingest key (read/write) - for ingest services that write to Elasticsearch
#   2. Readonly key (read-only) - for API services that only query Elasticsearch

set -e

GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"
GE_K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"
GE_K8S_CLUSTER="greenearth-${GE_ENVIRONMENT}-cluster"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"

echo "Creating new Elasticsearch API keys..."
echo "Environment: ${GE_ENVIRONMENT}"
echo "Namespace: ${GE_K8S_NAMESPACE}"
echo ""

# Set up kubectl context for the target environment
echo "Setting kubectl context for ${GE_ENVIRONMENT} environment..."
gcloud container clusters get-credentials "$GE_K8S_CLUSTER" \
    --location="$GE_GCP_REGION" \
    --project="$GE_GCP_PROJECT_ID"

echo ""

# Get elastic superuser credentials (required for creating API keys)
ELASTICSEARCH_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "${GE_K8S_NAMESPACE}" -o jsonpath='{.data.elastic}' | base64 -d)

if [ -z "$ELASTICSEARCH_PASSWORD" ]; then
  echo "Error: Could not retrieve elastic superuser password from secret"
  exit 1
fi

# Determine pod name based on environment
# Prod has dedicated data nodes, stage has data-only nodes
if [ "$GE_ENVIRONMENT" = "prod" ]; then
    ES_POD="greenearth-es-data-0"
else
    ES_POD="greenearth-es-data-only-0"
fi

echo "Using pod: ${ES_POD}"
echo ""

# Determine secret name suffix based on environment
# Stage uses no suffix for backwards compatibility, prod uses -prod suffix
SECRET_SUFFIX=""
if [ "$GE_ENVIRONMENT" = "prod" ]; then
    SECRET_SUFFIX="-prod"
fi

# =============================================================================
# 1. Create INGEST API key (read/write access for ingest services)
# =============================================================================
echo "=========================================="
echo "Creating INGEST API key (read/write)..."
echo "=========================================="

INGEST_KEY_RESPONSE=$(kubectl exec -n "${GE_K8S_NAMESPACE}" "${ES_POD}" -- curl -k -s -X POST \
  -u "elastic:${ELASTICSEARCH_PASSWORD}" \
  "https://localhost:9200/_security/api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ingest-services",
    "expiration": "365d",
    "role_descriptors": {
      "ingest_role": {
        "cluster": ["monitor", "manage_index_templates"],
        "indices": [
          {
            "names": ["posts", "posts_*", "likes", "likes_*", "post_tombstones", 
              "post_tombstones_*", "like_tombstones", "like_tombstones_*", "hashtags", "hashtags*"],
            "privileges": ["all", "maintenance", "create_index", "auto_configure"]
          }
        ]
      }
    }
  }')

echo "Ingest API Key Response:"
echo "$INGEST_KEY_RESPONSE"
echo ""

# Extract the encoded API key
INGEST_ENCODED_KEY=$(echo "$INGEST_KEY_RESPONSE" | grep -o '"encoded":"[^"]*"' | cut -d'"' -f4)

if [ -z "$INGEST_ENCODED_KEY" ]; then
  echo "Error: Failed to create ingest API key"
  exit 1
fi

echo "Created ingest API key (base64 encoded): $INGEST_ENCODED_KEY"

# Store ingest key in Secret Manager
INGEST_SECRET_NAME="elasticsearch-api-key${SECRET_SUFFIX}"
echo "Storing ingest key in secret: $INGEST_SECRET_NAME"

# Check if secret exists, create if not
if ! gcloud secrets describe "$INGEST_SECRET_NAME" > /dev/null 2>&1; then
    echo -n "$INGEST_ENCODED_KEY" | gcloud secrets create "$INGEST_SECRET_NAME" --data-file=-
else
    echo -n "$INGEST_ENCODED_KEY" | gcloud secrets versions add "$INGEST_SECRET_NAME" --data-file=-
fi

echo ""

# =============================================================================
# 2. Create READONLY API key (read-only access for API services)
# =============================================================================
echo "=========================================="
echo "Creating READONLY API key (read-only)..."
echo "=========================================="

READONLY_KEY_RESPONSE=$(kubectl exec -n "${GE_K8S_NAMESPACE}" "${ES_POD}" -- curl -k -s -X POST \
  -u "elastic:${ELASTICSEARCH_PASSWORD}" \
  "https://localhost:9200/_security/api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api-services-readonly",
    "expiration": "365d",
    "role_descriptors": {
      "readonly_role": {
        "cluster": ["monitor"],
        "indices": [
          {
            "names": ["posts", "posts_*", "likes", "likes_*", "post_tombstones", 
              "post_tombstones_*", "like_tombstones", "like_tombstones_*", "hashtags", "hashtags*"],
            "privileges": ["read", "view_index_metadata"]
          }
        ]
      }
    }
  }')

echo "Readonly API Key Response:"
echo "$READONLY_KEY_RESPONSE"
echo ""

# Extract the encoded API key
READONLY_ENCODED_KEY=$(echo "$READONLY_KEY_RESPONSE" | grep -o '"encoded":"[^"]*"' | cut -d'"' -f4)

if [ -z "$READONLY_ENCODED_KEY" ]; then
  echo "Error: Failed to create readonly API key"
  exit 1
fi

echo "Created readonly API key (base64 encoded): $READONLY_ENCODED_KEY"

# Store readonly key in Secret Manager
READONLY_SECRET_NAME="elasticsearch-api-key-readonly${SECRET_SUFFIX}"
echo "Storing readonly key in secret: $READONLY_SECRET_NAME"

# Check if secret exists, create if not
if ! gcloud secrets describe "$READONLY_SECRET_NAME" > /dev/null 2>&1; then
    echo -n "$READONLY_ENCODED_KEY" | gcloud secrets create "$READONLY_SECRET_NAME" --data-file=-
else
    echo -n "$READONLY_ENCODED_KEY" | gcloud secrets versions add "$READONLY_SECRET_NAME" --data-file=-
fi

echo ""
echo "=========================================="
echo "Done! API keys have been recreated and stored in Secret Manager."
echo "=========================================="
echo ""
echo "Secrets created/updated:"
echo "  - $INGEST_SECRET_NAME (read/write - for ingest services)"
echo "  - $READONLY_SECRET_NAME (read-only - for API services)"
echo ""
echo "You may need to redeploy services for them to pick up the new keys:"
echo ""
echo "  Ingest services:"
echo "    cd /Users/raindrift/src/ingex/ingest"
echo "    ./scripts/deploy.sh jetstream"
echo "    ./scripts/deploy.sh megastream"
echo "    ./scripts/deploy.sh expiry"
echo ""
echo "  API service:"
echo "    cd /Users/raindrift/src/api"
echo "    ./scripts/deploy.sh"
