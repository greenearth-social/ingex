#!/bin/bash
# Delete and recreate Elasticsearch indices to free up disk space
# This is the recommended approach when disk is too full for delete-by-query
# See: https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors

set -e

GE_ENVIRONMENT="${GE_ENVIRONMENT:-local}"
GE_K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"
GE_K8S_CLUSTER="greenearth-${GE_ENVIRONMENT}-cluster"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"

# Set up kubectl context for the target environment
if [ "$GE_ENVIRONMENT" != "local" ]; then
    echo "Setting kubectl context for ${GE_ENVIRONMENT} environment..."
    gcloud container clusters get-credentials "$GE_K8S_CLUSTER" \
        --location="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID"
    echo ""
fi

echo "Running ES index deletion and recreation in ${GE_ENVIRONMENT} environment (namespace: ${GE_K8S_NAMESPACE})"
echo ""

# Get the elastic superuser credentials (needed for index deletion)
ELASTICSEARCH_USERNAME="elastic"
ELASTICSEARCH_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "${GE_K8S_NAMESPACE}" -o jsonpath='{.data.elastic}' | base64 -d)

if [ -z "$ELASTICSEARCH_PASSWORD" ]; then
  echo "Error: Could not retrieve elastic superuser password"
  exit 1
fi

echo "Using elastic superuser (required for index deletion)"
echo ""

# The internal service name for Elasticsearch (HTTPS)
GE_ELASTICSEARCH_URL="https://greenearth-es-http:9200"

echo "Will connect to: ${GE_ELASTICSEARCH_URL}"
echo "WARNING: This will DELETE and RECREATE all data indices!"
echo "This deletes the actual indices: posts_v1, likes_v1, post_tombstones_v1, like_tombstones_v1, hashtags_v1"
echo "The indices will be recreated from templates automatically."
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# Stop ingest services for stage/prod to prevent them from writing during recreation
if [ "$GE_ENVIRONMENT" = "stage" ] || [ "$GE_ENVIRONMENT" = "prod" ]; then
  echo ""
  echo "Stopping ingest services to prevent writes during recreation..."

  # Find ingestctl script
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  INGESTCTL="${SCRIPT_DIR}/ingestctl.sh"

  if [ -f "$INGESTCTL" ]; then
    # Stop all services
    "$INGESTCTL" stop || {
      echo "Warning: Failed to stop services. Continuing anyway..."
    }
    echo "Ingest services stopped. Waiting 10 seconds for in-flight requests to complete..."
    sleep 10
  else
    echo "Warning: ingestctl.sh not found at ${INGESTCTL}"
    echo "Services will not be stopped. This may cause alias conflicts."
    read -p "Continue anyway? (type 'yes' to confirm): " continue_confirm
    if [ "$continue_confirm" != "yes" ]; then
      echo "Aborted."
      exit 0
    fi
  fi
  echo ""
fi

# Create a job that deletes and recreates the indices
kubectl run es-index-recreation-$(date +%s) \
  --namespace="${GE_K8S_NAMESPACE}" \
  --image=curlimages/curl:latest \
  --restart=Never \
  --rm -i --tty \
  --env="GE_ELASTICSEARCH_URL=${GE_ELASTICSEARCH_URL}" \
  --env="ELASTICSEARCH_USERNAME=${ELASTICSEARCH_USERNAME}" \
  --env="ELASTICSEARCH_PASSWORD=${ELASTICSEARCH_PASSWORD}" \
  -- sh -c '
echo "Removing read-only blocks from all indices..."
curl -k -X PUT "${GE_ELASTICSEARCH_URL}/_all/_settings" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"index.blocks.read_only_allow_delete\": null}"

echo ""
echo ""
echo "Deleting posts_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/posts_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting likes_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/likes_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting post_tombstones_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/post_tombstones_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting like_tombstones_v1 index (if exists)..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/like_tombstones_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  2>/dev/null || echo "like_tombstones_v1 does not exist, skipping"

echo ""
echo ""
echo "Deleting hashtags_v1 index (if exists)..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/hashtags_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  2>/dev/null || echo "hashtags_v1 does not exist, skipping"

echo ""
echo ""
echo "Resetting disk watermark settings to defaults..."
curl -k -X PUT "${GE_ELASTICSEARCH_URL}/_cluster/settings" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{
    \"persistent\": {
      \"cluster.routing.allocation.disk.watermark.low\": null,
      \"cluster.routing.allocation.disk.watermark.high\": null,
      \"cluster.routing.allocation.disk.watermark.flood_stage\": null
    },
    \"transient\": {
      \"cluster.routing.allocation.disk.watermark.low\": null,
      \"cluster.routing.allocation.disk.watermark.high\": null,
      \"cluster.routing.allocation.disk.watermark.flood_stage\": null
    }
  }"

echo ""
echo ""
echo "Verifying cluster health..."
curl -k -X GET "${GE_ELASTICSEARCH_URL}/_cluster/health?pretty" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deletion complete! Indices removed."
'

echo ""
echo "Next, recreate indices using index/deploy.sh $GE_ENVIRONMENT --ctype schema"

# Restart ingest services for stage/prod
if [ "$GE_ENVIRONMENT" = "stage" ] || [ "$GE_ENVIRONMENT" = "prod" ]; then
  echo ""
  echo "To restart ingest services, run ingetsctl.sh start"
fi

echo ""
echo "Check disk space with:"
ES_POD=$(kubectl get pods -n "${GE_K8S_NAMESPACE}" -l common.k8s.elastic.co/type=elasticsearch -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$ES_POD" ]; then
  echo "kubectl exec -n ${GE_K8S_NAMESPACE} ${ES_POD} -- df -h /usr/share/elasticsearch/data"
else
  echo "kubectl exec -n ${GE_K8S_NAMESPACE} <elasticsearch-pod-name> -- df -h /usr/share/elasticsearch/data"
  echo "(Run 'kubectl get pods -n ${GE_K8S_NAMESPACE}' to find the Elasticsearch pod name)"
fi

echo ""
echo "If receiving '...index has read-only-allow-delete block...' errors, also run ./fix_es_readonly.sh"