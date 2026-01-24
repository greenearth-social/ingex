#!/bin/bash
# Recreate Elasticsearch API key for Cloud Run services

set -e

GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"
GE_K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"
GE_K8S_CLUSTER="greenearth-${GE_ENVIRONMENT}-cluster"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"

echo "Creating new Elasticsearch API key..."
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

echo "Creating API key with permissions for ingest services..."
echo "Using pod: ${ES_POD}"

# Create API key with full permissions for all indices
API_KEY_RESPONSE=$(kubectl exec -n "${GE_K8S_NAMESPACE}" "${ES_POD}" -- curl -k -s -X POST \
  -u "elastic:${ELASTICSEARCH_PASSWORD}" \
  "https://localhost:9200/_security/api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cloud-run-services",
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

echo ""
echo "API Key Response:"
echo "$API_KEY_RESPONSE"
echo ""

# Extract the encoded API key
ENCODED_KEY=$(echo "$API_KEY_RESPONSE" | grep -o '"encoded":"[^"]*"' | cut -d'"' -f4)

if [ -z "$ENCODED_KEY" ]; then
  echo "Error: Failed to create API key"
  exit 1
fi

echo "Created API key (base64 encoded): $ENCODED_KEY"
echo ""
echo "Updating Google Secret Manager..."

# Determine secret name based on environment
# Stage uses no suffix for backwards compatibility, prod uses -prod suffix
SECRET_NAME="elasticsearch-api-key"
if [ "$GE_ENVIRONMENT" = "prod" ]; then
    SECRET_NAME="elasticsearch-api-key-prod"
fi

# Update the secret in Google Secret Manager
echo "Storing in secret: $SECRET_NAME"
echo -n "$ENCODED_KEY" | gcloud secrets versions add "$SECRET_NAME" --data-file=-

echo ""
echo "Done! API key has been recreated and stored in Secret Manager."
echo ""
echo "You may need to redeploy services for them to pick up the new key:"
echo "  cd /Users/raindrift/src/ingex/ingest"
echo "  ./scripts/deploy.sh jetstream"
echo "  ./scripts/deploy.sh megastream"
echo "  ./scripts/deploy.sh expiry"
