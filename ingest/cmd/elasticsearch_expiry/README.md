# Elasticsearch Expiry Service

A Go service for automatically expiring (deleting) old documents from Elasticsearch collections. Designed to run as a scheduled task on GCP.

## Collections Processed

The service automatically processes these collections:

| Collection | Index Alias | Date Field | Description |
|------------|-------------|------------|-------------|
| Posts | `posts` | `created_at` | BlueSky posts and threads |
| Likes | `likes` | `created_at` | User likes on posts |
| Post Tombstones | `post_tombstones` | `deleted_at` | Records of deleted posts |

## Configuration

Configuration is done through environment variables:

### Required

- `GE_ELASTICSEARCH_URL` - Elasticsearch cluster endpoint (e.g., `https://localhost:9200`)
- `GE_ELASTICSEARCH_API_KEY` - Elasticsearch API key with delete permissions (not required in dry-run mode)

### Optional

- `GE_LOGGING_ENABLED` - Enable/disable detailed logging (default: `true`)

### Command Line Options

- `--dry-run` - Run in dry-run mode (show what would be deleted without actually deleting)
- `--skip-tls-verify` - Skip TLS certificate verification (use for local development only)
- `--retention-hours` - Number of hours to retain data (default: `1440` hours = 60 days)

## Required Elasticsearch Permissions

The API key needs the following permissions:

```json
{
  "indices": [
    {
      "names": ["posts", "posts_v1", "likes", "likes_v1", "post_tombstones", "post_tombstones_v1"],
      "privileges": ["read", "delete"]
    }
  ]
}
```

### Getting an Existing API Key

If you already have an API key created for other ingest services, you can retrieve it from your Kubernetes cluster:

```bash
# Get the API key from the ingest service secret (if it exists)
kubectl get secret ingest-service-key -n greenearth-local -o go-template='{{.data.api_key | base64decode}}'

# Or check for other API key secrets
kubectl get secrets -n greenearth-local | grep api-key
```

### Creating a New API Key via Kibana

1. Access Kibana at your cluster URL
2. Navigate to **Stack Management → Security → API Keys**
3. Click **Create API key**
4. Set name: `elasticsearch-expiry-key`
5. Configure with the permissions above
6. Copy the encoded API key

### Creating a New API Key via CLI

```bash
# Port-forward to Elasticsearch (if using local cluster)
kubectl port-forward service/greenearth-es-http 9200 -n greenearth-local &

# Get elastic password
ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n greenearth-local -o go-template='{{.data.elastic | base64decode}}')

# Create API key (same as used by other ingest commands)
curl -k -X POST "https://localhost:9200/_security/api_key" \
  -u "elastic:$ELASTIC_PASSWORD" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-expiry-key",
    "role_descriptors": {
      "expiry_role": {
        "indices": [
          {
            "names": ["posts", "posts_v1", "likes", "likes_v1", "post_tombstones", "post_tombstones_v1"],
            "privileges": ["read", "delete"]
          }
        ]
      }
    }
  }'
```

## Usage

### Local Development

```bash
# Build the service
cd /Users/raindrift/src/ingex/ingest
go build -o bin/elasticsearch_expiry ./cmd/elasticsearch_expiry

# Test with dry-run (safe to run, won't delete anything)
export GE_ELASTICSEARCH_URL="https://localhost:9200"
export GE_ELASTICSEARCH_API_KEY="your-api-key-here"
export GE_LOGGING_ENABLED="true"

./bin/elasticsearch_expiry --dry-run --skip-tls-verify

# Run with custom retention (720 hours = 30 days instead of default 1440 hours)
./bin/elasticsearch_expiry --dry-run --retention-hours 720
```

### Production Usage

```bash
# Set environment variables
export GE_ELASTICSEARCH_URL="https://your-cluster.es.amazonaws.com:9200"
export GE_ELASTICSEARCH_API_KEY="your-production-api-key"
export GE_LOGGING_ENABLED="true"

# Run the expiry process
./elasticsearch_expiry
```

## Troubleshooting

### Permission Errors

If you see authentication or permission errors:

1. Verify the `GE_ELASTICSEARCH_API_KEY` is correctly set
2. Ensure the API key has `read` and `delete` permissions on all relevant indices
3. Check that the Elasticsearch cluster is accessible from your deployment environment

### Performance Issues

If the expiry process is too slow or overwhelming your cluster:

1. Run during off-peak hours when cluster load is lower
2. Consider running more frequently with shorter retention periods (e.g., hourly with 5-hour retention for limited capacity clusters)
3. Monitor Elasticsearch cluster health during operations
4. Use `--dry-run` first to estimate the impact

### No Documents Deleted

If the service runs but doesn't delete anything:

1. Run with `--dry-run` to see what would be deleted
2. Check the retention period with `--retention-hours`
3. Verify the date fields in your indices match the expected format (ISO8601)
4. Enable debug logging and check the search queries being executed
