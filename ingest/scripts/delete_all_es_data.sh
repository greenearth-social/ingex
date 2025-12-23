#!/bin/bash
# Delete all data from Elasticsearch indices
# Use with caution - this will delete ALL documents!

set -e

GE_ELASTICSEARCH_URL="${GE_ELASTICSEARCH_URL:-http://localhost:9200}"
GE_ELASTICSEARCH_API_KEY="${GE_ELASTICSEARCH_API_KEY}"

if [ -z "$GE_ELASTICSEARCH_API_KEY" ]; then
  echo "Error: GE_ELASTICSEARCH_API_KEY environment variable not set"
  exit 1
fi

echo "WARNING: This will delete ALL data from Elasticsearch indices!"
echo "GE_ELASTICSEARCH_URL: ${GE_ELASTICSEARCH_URL}"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# First, remove any read-only blocks
echo ""
echo "Removing read-only blocks..."
curl -X PUT "${GE_ELASTICSEARCH_URL}/_all/_settings" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "index.blocks.read_only_allow_delete": null
  }'

echo ""
echo ""
echo "Deleting all documents from posts index..."
curl -X POST "${GE_ELASTICSEARCH_URL}/posts/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match_all": {}
    }
  }'

echo ""
echo ""
echo "Deleting all documents from likes index..."
curl -X POST "${GE_ELASTICSEARCH_URL}/likes/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match_all": {}
    }
  }'

echo ""
echo ""
echo "Deleting all documents from post-tombstones index..."
curl -X POST "${GE_ELASTICSEARCH_URL}/post-tombstones/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match_all": {}
    }
  }'

echo ""
echo ""
echo "Deletion tasks started asynchronously."
echo ""
echo "Check progress with:"
echo "curl -X GET \"${GE_ELASTICSEARCH_URL}/_tasks?detailed=true&actions=*delete/byquery\" -H \"Authorization: ApiKey \${GE_ELASTICSEARCH_API_KEY}\""
echo ""
echo "Check disk usage with:"
echo "curl -X GET \"${GE_ELASTICSEARCH_URL}/_cat/allocation?v\" -H \"Authorization: ApiKey \${GE_ELASTICSEARCH_API_KEY}\""
echo ""
echo "Check index sizes with:"
echo "curl -X GET \"${GE_ELASTICSEARCH_URL}/_cat/indices?v\" -H \"Authorization: ApiKey \${GE_ELASTICSEARCH_API_KEY}\""
