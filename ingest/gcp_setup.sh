#!/bin/bash

# Green Earth Ingex - GCP Environment Setup Script
# This script sets up the GCP environment for the first time
# Run this once per environment (dev, stage, prod)

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-prod}"  # TODO: change default when we have more environments

# Elasticsearch configuration - update these values
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-https://your-elasticsearch-cluster:9200}"
ELASTICSEARCH_API_KEY="${ELASTICSEARCH_API_KEY:-your-api-key}"

# S3 configuration for Megastream data
S3_BUCKET="${S3_BUCKET:-your-megastream-bucket}"
S3_PREFIX="${S3_PREFIX:-megastream/databases/}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    if ! command -v kubectl &> /dev/null; then
        log_warn "kubectl is not installed. You may need it for debugging."
    fi

    # Check if user is logged in
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        log_error "Please log in to gcloud first: gcloud auth login"
        exit 1
    fi

    log_info "Prerequisites check complete."
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set PROJECT_ID environment variable or update the script"
        exit 1
    fi

    if [ "$ELASTICSEARCH_URL" = "https://your-elasticsearch-cluster:9200" ]; then
        log_error "Please set ELASTICSEARCH_URL environment variable or update the script"
        exit 1
    fi

    if [ "$ELASTICSEARCH_API_KEY" = "your-api-key" ]; then
        log_error "Please set ELASTICSEARCH_API_KEY environment variable or update the script"
        exit 1
    fi

    log_info "Configuration validation complete."
}

setup_gcp_project() {
    log_info "Setting up GCP project: $PROJECT_ID"

    # Set the project
    gcloud config set project "$PROJECT_ID"

    # Enable required APIs
    log_info "Enabling required GCP APIs..."
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        scheduler.googleapis.com \
        secretmanager.googleapis.com \
        storage.googleapis.com \
        artifactregistry.googleapis.com

    log_info "GCP project setup complete."
}

create_artifact_registry() {
    log_info "Creating Artifact Registry repository..."

    # Create repository for container images
    if ! gcloud artifacts repositories describe ingex --location="$REGION" > /dev/null 2>&1; then
        gcloud artifacts repositories create ingex \
            --repository-format=docker \
            --location="$REGION" \
            --description="Green Earth Ingex container images"
        log_info "Artifact Registry repository created."
    else
        log_info "Artifact Registry repository already exists."
    fi
}

create_service_account() {
    log_info "Creating service account for Cloud Run services..."

    SA_NAME="ingex-runner-$ENVIRONMENT"
    SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

    # Create service account
    if ! gcloud iam service-accounts describe "$SA_EMAIL" > /dev/null 2>&1; then
        gcloud iam service-accounts create "$SA_NAME" \
            --display-name="Ingex Cloud Run Service Account ($ENVIRONMENT)" \
            --description="Service account for running ingex services in $ENVIRONMENT"
        log_info "Service account created: $SA_EMAIL"
    else
        log_info "Service account already exists: $SA_EMAIL"
    fi

    # Grant necessary permissions
    log_info "Granting permissions to service account..."

    # Permission to read from Cloud Storage (for S3-compatible access)
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/storage.objectViewer"

    # Permission to access secrets
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/secretmanager.secretAccessor"

    # Permission for Cloud Run to run jobs
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/run.invoker"

    log_info "Service account permissions configured."
}

create_secrets() {
    log_info "Creating secrets in Secret Manager..."

    # Elasticsearch configuration
    if ! gcloud secrets describe elasticsearch-config > /dev/null 2>&1; then
        echo -n "$ELASTICSEARCH_URL" | gcloud secrets create elasticsearch-url --data-file=-
        echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets create elasticsearch-api-key --data-file=-
        log_info "Elasticsearch secrets created."
    else
        log_info "Elasticsearch secrets already exist. Updating..."
        echo -n "$ELASTICSEARCH_URL" | gcloud secrets versions add elasticsearch-url --data-file=-
        echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets versions add elasticsearch-api-key --data-file=-
    fi

    # S3 configuration
    if ! gcloud secrets describe s3-bucket > /dev/null 2>&1; then
        echo -n "$S3_BUCKET" | gcloud secrets create s3-bucket --data-file=-
        echo -n "$S3_PREFIX" | gcloud secrets create s3-prefix --data-file=-
        log_info "S3 configuration secrets created."
    else
        log_info "S3 configuration secrets already exist. Updating..."
        echo -n "$S3_BUCKET" | gcloud secrets versions add s3-bucket --data-file=-
        echo -n "$S3_PREFIX" | gcloud secrets versions add s3-prefix --data-file=-
    fi
}

create_persistent_storage() {
    log_info "Setting up persistent storage for state files..."

    # Create a Cloud Storage bucket for state files
    BUCKET_NAME="$PROJECT_ID-ingex-state-$ENVIRONMENT"

    if ! gsutil ls -b gs://"$BUCKET_NAME" > /dev/null 2>&1; then
        gsutil mb -l "$REGION" gs://"$BUCKET_NAME"
        log_info "Storage bucket created: $BUCKET_NAME"
    else
        log_info "Storage bucket already exists: $BUCKET_NAME"
    fi

    # Set appropriate permissions
    gsutil iam ch serviceAccount:"ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com":objectAdmin gs://"$BUCKET_NAME"
}

setup_cloud_scheduler() {
    log_info "Setting up Cloud Scheduler for elasticsearch-expiry..."

    JOB_NAME="elasticsearch-expiry-daily-$ENVIRONMENT"
    SERVICE_ACCOUNT="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com"
    JOB_URI="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/elasticsearch-expiry:run"

    # Create the scheduler job (runs daily at 2 AM UTC)
    if ! gcloud scheduler jobs describe "$JOB_NAME" --location="$REGION" > /dev/null 2>&1; then
        gcloud scheduler jobs create http "$JOB_NAME" \
            --location="$REGION" \
            --schedule="0 2 * * *" \
            --uri="$JOB_URI" \
            --http-method=POST \
            --oidc-service-account-email="$SERVICE_ACCOUNT" \
            --oidc-token-audience="$JOB_URI" \
            --description="Daily Elasticsearch data expiry for $ENVIRONMENT"
        log_info "Cloud Scheduler job created: $JOB_NAME"
    else
        log_info "Cloud Scheduler job already exists: $JOB_NAME"
    fi
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - GCP Environment Setup"
    echo "Environment: $ENVIRONMENT"
    echo "Project: $PROJECT_ID"
    echo "Region: $REGION"
    echo "=================================================="
    echo

    check_prerequisites
    validate_config
    setup_gcp_project
    create_artifact_registry
    create_service_account
    create_secrets
    create_persistent_storage
    setup_cloud_scheduler

    log_info "Environment setup complete!"
    echo
    echo "Next steps:"
    echo "1. Run './deploy.sh' to build and deploy your services"
    echo "2. Check Cloud Run console to verify services are running"
    echo "3. Monitor logs for any issues"
    echo
    echo "Important notes:"
    echo "- Elasticsearch expiry runs daily at 2 AM UTC"
    echo "- State files are stored in: gs://$PROJECT_ID-ingex-state-$ENVIRONMENT"
    echo "- Service account: ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com"
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
        --elasticsearch-url)
            ELASTICSEARCH_URL="$2"
            shift 2
            ;;
        --elasticsearch-api-key)
            ELASTICSEARCH_API_KEY="$2"
            shift 2
            ;;
        --s3-bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        --s3-prefix)
            S3_PREFIX="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --project-id ID           GCP project ID"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --environment ENV        Environment name (default: prod)"
            echo "  --elasticsearch-url URL  Elasticsearch cluster URL"
            echo "  --elasticsearch-api-key KEY  Elasticsearch API key"
            echo "  --s3-bucket BUCKET       S3 bucket for Megastream data"
            echo "  --s3-prefix PREFIX       S3 prefix for Megastream data"
            echo "  --help                   Show this help message"
            echo
            echo "You can also set these values via environment variables:"
            echo "  PROJECT_ID, REGION, ENVIRONMENT, ELASTICSEARCH_URL, ELASTICSEARCH_API_KEY, S3_BUCKET, S3_PREFIX"
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
