#!/bin/bash
# Temporarily remove read-only block from Elasticsearch indices
# This should only be used after ensuring there's enough disk space

ES_HOST="${ES_HOST:-http://localhost:9200}"
ES_API_KEY="${ES_API_KEY}"

if [ -z "$ES_API_KEY" ]; then
  echo "Error: ES_API_KEY environment variable not set"
  exit 1
fi

echo "Removing read-only block from all indices..."
curl -X PUT "${ES_HOST}/_all/_settings" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "index.blocks.read_only_allow_delete": null
  }'

echo ""
echo "Checking cluster status..."
curl -X GET "${ES_HOST}/_cluster/health?pretty" \
  -H "Authorization: ApiKey ${ES_API_KEY}"

echo ""
echo "Checking disk usage..."
curl -X GET "${ES_HOST}/_cat/allocation?v&h=node,disk.used_percent,disk.used,disk.avail,disk.total" \
  -H "Authorization: ApiKey ${ES_API_KEY}"
