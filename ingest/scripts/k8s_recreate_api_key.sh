#!/bin/bash
# Recreate Elasticsearch API key for Cloud Run services

set -e

GE_ENVIRONMENT="${1:-stage}"
K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"

echo "Creating new Elasticsearch API key..."
echo "Environment: ${GE_ENVIRONMENT}"
echo "Namespace: ${K8S_NAMESPACE}"
echo ""

# Get elastic superuser credentials (required for creating API keys)
ELASTICSEARCH_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "${K8S_NAMESPACE}" -o jsonpath='{.data.elastic}' | base64 -d)

if [ -z "$ELASTICSEARCH_PASSWORD" ]; then
  echo "Error: Could not retrieve elastic superuser password from secret"
  exit 1
fi

echo "Creating API key with permissions for ingest services..."

# Create API key with full permissions for all indices
API_KEY_RESPONSE=$(kubectl exec -n "${K8S_NAMESPACE}" greenearth-es-data-only-0 -- curl -k -s -X POST \
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
            "names": ["posts", "posts_*", "likes", "likes_*", "post_tombstones", "post_tombstones_*", "like_tombstones", "like_tombstones_*"],
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

# Update the secret in Google Secret Manager
echo -n "$ENCODED_KEY" | gcloud secrets versions add elasticsearch-api-key --data-file=-

echo ""
echo "Done! API key has been recreated and stored in Secret Manager."
echo ""
echo "You may need to redeploy services for them to pick up the new key:"
echo "  cd /Users/raindrift/src/ingex/ingest"
echo "  ./scripts/deploy.sh jetstream"
echo "  ./scripts/deploy.sh megastream"
echo "  ./scripts/deploy.sh expiry"
