#!/bin/bash

# Green Earth Ingex - GCP Environment Setup Script
# This script sets up the GCP environment for the first time
# Run this once per environment (dev, stage, prod])

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-stage}"  # TODO: change default when we have more environments

# Elasticsearch configuration - only API key is secret, URL is public
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-INTERNAL_LB_PLACEHOLDER}"
ELASTICSEARCH_API_KEY="${ELASTICSEARCH_API_KEY:-your-api-key}"

# S3 configuration for Megastream data
# TODO: actual s3 bucket name
S3_BUCKET="${S3_BUCKET:-greenearth-megastream-data}"
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

    if [ "$ELASTICSEARCH_API_KEY" = "your-api-key" ]; then
        log_error "Please set ELASTICSEARCH_API_KEY environment variable - this is the only required secret"
        log_error "Other configuration (Elasticsearch URL, S3 bucket/prefix) now have defaults in the scripts"
        exit 1
    fi

    log_info "Configuration validation complete."
    log_info "Using Elasticsearch URL: $ELASTICSEARCH_URL"
    log_info "Using S3 bucket: $S3_BUCKET with prefix: $S3_PREFIX"
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
        cloudscheduler.googleapis.com \
        secretmanager.googleapis.com \
        storage.googleapis.com \
        artifactregistry.googleapis.com \
        vpcaccess.googleapis.com \
        compute.googleapis.com

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

    # Permission to use VPC connectors
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/vpcaccess.user"

    log_info "Service account permissions configured."
}

create_secrets() {
    log_info "Creating secrets in Secret Manager..."

    # Only store actual secrets - Elasticsearch API key
    if ! gcloud secrets describe elasticsearch-api-key > /dev/null 2>&1; then
        echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets create elasticsearch-api-key --data-file=-
        log_info "Elasticsearch API key secret created."
    else
        log_info "Elasticsearch API key secret already exists. Updating..."
        echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets versions add elasticsearch-api-key --data-file=-
        log_info "Elasticsearch API key secret updated."
    fi

    log_info "Note: Non-secret configuration (Elasticsearch URL, S3 bucket, S3 prefix) is now stored in the deployment scripts."
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

create_vpc_connector() {
    log_info "Creating VPC connector for Cloud Run services..."

    CONNECTOR_NAME="ingex-vpc-connector-$ENVIRONMENT"

    # Check if VPC connector already exists
    if gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$REGION" > /dev/null 2>&1; then
        log_info "VPC connector already exists: $CONNECTOR_NAME"
        return
    fi

    # Create VPC connector service account if it doesn't exist
    CONNECTOR_SA_NAME="vpc-connector-sa-$ENVIRONMENT"
    CONNECTOR_SA_EMAIL="$CONNECTOR_SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

    if ! gcloud iam service-accounts describe "$CONNECTOR_SA_EMAIL" > /dev/null 2>&1; then
        gcloud iam service-accounts create "$CONNECTOR_SA_NAME" \
            --display-name="VPC Connector Service Account ($ENVIRONMENT)" \
            --description="Service account for VPC connector in $ENVIRONMENT"
        log_info "VPC connector service account created: $CONNECTOR_SA_EMAIL"
    fi

    # Create VPC connector
    # Use a small IP range for the connector (only needs a few IPs for Cloud Run)
    # Using 192.168.1.0/28 to avoid conflicts with existing subnets
    gcloud compute networks vpc-access connectors create "$CONNECTOR_NAME" \
        --network=default \
        --region="$REGION" \
        --range=192.168.1.0/28 \
        --min-instances=2 \
        --max-instances=10 \
        --machine-type=e2-micro

    log_info "VPC connector created: $CONNECTOR_NAME"

    # Grant the default Cloud Run service account permission to use VPC connectors
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$PROJECT_ID-compute@developer.gserviceaccount.com" \
        --role="roles/vpcaccess.user"

    log_info "VPC connector permissions configured"
}

setup_firewall_rules() {
    log_info "Setting up firewall rules for VPC access..."

    # Allow Cloud Run services to access Elasticsearch through internal load balancer
    FIREWALL_RULE_NAME="allow-cloud-run-to-elasticsearch-$ENVIRONMENT"

    if ! gcloud compute firewall-rules describe "$FIREWALL_RULE_NAME" > /dev/null 2>&1; then
        gcloud compute firewall-rules create "$FIREWALL_RULE_NAME" \
            --network=default \
            --allow=tcp:9200,tcp:9300 \
            --source-ranges=192.168.1.0/28 \
            --target-tags=gke-greenearth-$ENVIRONMENT \
            --description="Allow Cloud Run services to access Elasticsearch internal load balancer"
        log_info "Firewall rule created: $FIREWALL_RULE_NAME"
    else
        log_info "Firewall rule already exists: $FIREWALL_RULE_NAME"
    fi

    # Allow internal load balancer health checks
    HEALTH_CHECK_RULE="allow-internal-lb-health-checks-$ENVIRONMENT"

    if ! gcloud compute firewall-rules describe "$HEALTH_CHECK_RULE" > /dev/null 2>&1; then
        gcloud compute firewall-rules create "$HEALTH_CHECK_RULE" \
            --network=default \
            --allow=tcp:9200 \
            --source-ranges=130.211.0.0/22,35.191.0.0/16 \
            --target-tags=gke-greenearth-$ENVIRONMENT \
            --description="Allow Google Cloud load balancer health checks"
        log_info "Health check firewall rule created: $HEALTH_CHECK_RULE"
    else
        log_info "Health check firewall rule already exists: $HEALTH_CHECK_RULE"
    fi
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
    create_vpc_connector
    setup_firewall_rules
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
            echo "  --project-id ID              GCP project ID"
            echo "  --region REGION              GCP region (default: us-east1)"
            echo "  --environment ENV            Environment name (default: stage)"
            echo "  --elasticsearch-url URL      Elasticsearch cluster URL (default: https://greenearth-es-http.greenearth-stage.svc.cluster.local:9200)"
            echo "  --elasticsearch-api-key KEY  Elasticsearch API key (REQUIRED - no default)"
            echo "  --s3-bucket BUCKET           S3 bucket for Megastream data (default: greenearth-megastream-data)"
            echo "  --s3-prefix PREFIX           S3 prefix for Megastream data (default: megastream/databases/)"
            echo "  --help                       Show this help message"
            echo
            echo "Required environment variables:"
            echo "  ELASTICSEARCH_API_KEY    Elasticsearch API key (only actual secret)"
            echo
            echo "Optional environment variables (have defaults):"
            echo "  PROJECT_ID, REGION, ENVIRONMENT, ELASTICSEARCH_URL, S3_BUCKET, S3_PREFIX"
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
