#!/bin/bash
# Temporarily remove read-only block from Elasticsearch indices
# This should only be used after ensuring there's enough disk space

GE_ELASTICSEARCH_URL="${GE_ELASTICSEARCH_URL:-http://localhost:9200}"
GE_ELASTICSEARCH_API_KEY="${GE_ELASTICSEARCH_API_KEY}"

if [ -z "$GE_ELASTICSEARCH_API_KEY" ]; then
  echo "Error: GE_ELASTICSEARCH_API_KEY environment variable not set"
  exit 1
fi

echo "Removing read-only block from all indices..."
curl -X PUT "${GE_ELASTICSEARCH_URL}/_all/_settings" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "index.blocks.read_only_allow_delete": null
  }'

echo ""
echo "Checking cluster status..."
curl -X GET "${GE_ELASTICSEARCH_URL}/_cluster/health?pretty" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}"

echo ""
echo "Checking disk usage..."
curl -X GET "${GE_ELASTICSEARCH_URL}/_cat/allocation?v&h=node,disk.used_percent,disk.used,disk.avail,disk.total" \
  -H "Authorization: ApiKey ${GE_ELASTICSEARCH_API_KEY}"
