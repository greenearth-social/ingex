#!/bin/bash

# Green Earth Ingex - Cloud Run Source Deployment Script
# This script deploys all ingex services to Google Cloud Run using source deployment
# Source deployment uses Google Cloud buildpacks to automatically build from Go source
#
# Prerequisites: Run gcp_setup.sh first to configure the GCP environment

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-prod}"  # TODO: change default when we have more environments

# Service configuration
JETSTREAM_MIN_INSTANCES="${JETSTREAM_MIN_INSTANCES:-1}"
JETSTREAM_MAX_INSTANCES="${JETSTREAM_MAX_INSTANCES:-1}"
MEGASTREAM_MIN_INSTANCES="${MEGASTREAM_MIN_INSTANCES:-1}"
MEGASTREAM_MAX_INSTANCES="${MEGASTREAM_MAX_INSTANCES:-1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_build() {
    echo -e "${BLUE}[BUILD]${NC} $1"
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set PROJECT_ID environment variable or use --project-id"
        exit 1
    fi

    # Set gcloud project
    gcloud config set project "$PROJECT_ID"

    log_info "Configuration validation complete."
}

deploy_jetstream_service() {
    log_info "Deploying jetstream-ingest service from source..."

    cd cmd/jetstream_ingest

    gcloud run deploy jetstream-ingest \
        --source=. \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-env-vars="JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --set-env-vars="JETSTREAM_STATE_FILE=/data/jetstream_state.json" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --min-instances="$JETSTREAM_MIN_INSTANCES" \
        --max-instances="$JETSTREAM_MAX_INSTANCES" \
        --cpu=1 \
        --memory=512Mi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated

    cd ../..
}

deploy_megastream_service() {
    log_info "Deploying megastream-ingest service from source..."

    cd cmd/megastream_ingest

    gcloud run deploy megastream-ingest \
        --source=. \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --set-env-vars="SPOOL_INTERVAL_SEC=300" \
        --set-env-vars="AWS_REGION=us-east-1" \
        --set-env-vars="MEGASTREAM_STATE_FILE=/data/megastream_state.json" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --set-secrets="S3_SQLITE_DB_BUCKET=s3-bucket:latest" \
        --set-secrets="S3_SQLITE_DB_PREFIX=s3-prefix:latest" \
        --min-instances="$MEGASTREAM_MIN_INSTANCES" \
        --max-instances="$MEGASTREAM_MAX_INSTANCES" \
        --cpu=1 \
        --memory=1Gi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated \
        --args="--source,s3,--mode,spool"

    cd ../..
}

deploy_expiry_job() {
    log_info "Deploying elasticsearch-expiry job from source..."

    cd cmd/elasticsearch_expiry

    gcloud run jobs deploy elasticsearch-expiry \
        --source=. \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --cpu=1 \
        --memory=512Mi \
        --task-timeout=3600 \
        --args="--retention-days,60"

    cd ../..
}

deploy_all_services() {
    log_info "Deploying all services to Cloud Run..."

    deploy_jetstream_service
    deploy_megastream_service
    deploy_expiry_job

    log_info "All services deployed successfully!"
}

show_service_status() {
    log_info "Checking service status..."

    echo
    echo "=== Cloud Run Services ==="
    gcloud run services list --region="$REGION" --filter="metadata.name:(jetstream-ingest OR megastream-ingest)"

    echo
    echo "=== Cloud Run Jobs ==="
    gcloud run jobs list --region="$REGION" --filter="metadata.name:elasticsearch-expiry"

    echo
    echo "=== Service URLs ==="
    local jetstream_url=$(gcloud run services describe jetstream-ingest --region="$REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")
    local megastream_url=$(gcloud run services describe megastream-ingest --region="$REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")

    echo "Jetstream Ingest: $jetstream_url"
    echo "Megastream Ingest: $megastream_url"
    echo

    log_info "Use 'gcloud run services logs read SERVICE_NAME --region=$REGION' to view logs"
    log_info "Use 'gcloud run jobs execute elasticsearch-expiry --region=$REGION' to manually run expiry"
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - Cloud Run Source Deployment"
    echo "Environment: $ENVIRONMENT"
    echo "Project: $PROJECT_ID"
    echo "Region: $REGION"
    echo "=================================================="
    echo

    validate_config
    deploy_all_services
    show_service_status

    log_info "Source deployment complete!"
    echo
    echo "Next steps:"
    echo "1. Check service logs to ensure they're running correctly"
    echo "2. Verify data is being ingested into Elasticsearch"
    echo "3. Test the expiry job manually if needed"
    echo
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --jetstream-instances)
            JETSTREAM_MIN_INSTANCES="$2"
            JETSTREAM_MAX_INSTANCES="$2"
            shift 2
            ;;
        --megastream-instances)
            MEGASTREAM_MIN_INSTANCES="$2"
            MEGASTREAM_MAX_INSTANCES="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Prerequisites:"
            echo "  Run gcp_setup.sh first to configure the GCP environment"
            echo
            echo "Options:"
            echo "  --project-id ID              GCP project ID"
            echo "  --region REGION             GCP region (default: us-east1)"
            echo "  --environment ENV           Environment name (default: prod)"
            echo "  --jetstream-instances N     Set min/max instances for jetstream service"
            echo "  --megastream-instances N    Set min/max instances for megastream service"
            echo "  --help                      Show this help message"
            echo
            echo "Environment variables:"
            echo "  PROJECT_ID                  GCP project ID"
            echo "  REGION                      GCP region"
            echo "  ENVIRONMENT                 Environment name"
            echo
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

main
