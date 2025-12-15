#!/bin/bash
# Temporarily remove read-only block from Elasticsearch indices
# This should only be used after ensuring there's enough disk space

ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
ELASTICSEARCH_API_KEY="${ELASTICSEARCH_API_KEY}"

if [ -z "$ELASTICSEARCH_API_KEY" ]; then
  echo "Error: ELASTICSEARCH_API_KEY environment variable not set"
  exit 1
fi

echo "Removing read-only block from all indices..."
curl -X PUT "${ELASTICSEARCH_URL}/_all/_settings" \
  -H "Authorization: ApiKey ${ELASTICSEARCH_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "index.blocks.read_only_allow_delete": null
  }'

echo ""
echo "Checking cluster status..."
curl -X GET "${ELASTICSEARCH_URL}/_cluster/health?pretty" \
  -H "Authorization: ApiKey ${ELASTICSEARCH_API_KEY}"

echo ""
echo "Checking disk usage..."
curl -X GET "${ELASTICSEARCH_URL}/_cat/allocation?v&h=node,disk.used_percent,disk.used,disk.avail,disk.total" \
  -H "Authorization: ApiKey ${ELASTICSEARCH_API_KEY}"
