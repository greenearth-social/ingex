#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_usage() {
    echo "Usage: $0"
    echo ""
    echo "Required Environment Variables:"
    echo "  GKE_PROJECT_ID      GCP project ID"
    echo "  GKE_REGION          GCP region (e.g., us-east1)"
    echo ""
    echo "Examples:"
    echo "  $0                  # Setup network"
}

log_info() {
    echo "[INFO] $1"
}

log_success() {
    echo "[SUCCESS] $1"
}

log_error() {
    echo "[ERROR] $1" >&2
}

log_warning() {
    echo "[WARNING] $1"
}

verify_prerequisites() {
    log_info "Verifying prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed"
        exit 1
    fi

    if [ -z "$GKE_PROJECT_ID" ]; then
        log_error "GKE_PROJECT_ID environment variable is not set"
        exit 1
    fi

    if [ -z "$GKE_REGION" ]; then
        log_error "GKE_REGION environment variable is not set"
        exit 1
    fi

    log_success "Prerequisites verified"
}

setup_cloud_router() {
    local router_name="greenearth-router"

    log_info "Checking if Cloud Router exists: $router_name"
    if gcloud compute routers describe "$router_name" \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Cloud Router already exists: $router_name"
    else
        log_info "Creating Cloud Router: $router_name"
        gcloud compute routers create "$router_name" \
            --network=default \
            --region="$GKE_REGION" \
            --project="$GKE_PROJECT_ID"
        log_success "Cloud Router created successfully"
    fi
}

setup_cloud_nat() {
    local router_name="greenearth-router"
    local nat_name="greenearth-nat"

    log_info "Checking if Cloud NAT exists: $nat_name"
    if gcloud compute routers nats describe "$nat_name" \
        --router="$router_name" \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Cloud NAT already exists: $nat_name"
    else
        log_info "Creating Cloud NAT: $nat_name"
        gcloud compute routers nats create "$nat_name" \
            --router="$router_name" \
            --region="$GKE_REGION" \
            --project="$GKE_PROJECT_ID" \
            --nat-all-subnet-ip-ranges \
            --auto-allocate-nat-external-ips
        log_success "Cloud NAT created successfully"
    fi
}

enable_private_google_access() {
    log_info "Enabling Private Google Access on default subnet..."

    gcloud compute networks subnets update default \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" \
        --enable-private-ip-google-access

    log_success "Private Google Access enabled"
}

main() {
    log_info "Setting up network..."
    log_info "Project: $GKE_PROJECT_ID"
    log_info "Region: $GKE_REGION"
    echo ""

    verify_prerequisites
    setup_cloud_router
    setup_cloud_nat
    enable_private_google_access

    echo ""
    log_success "Network setup complete"
    log_info "Cloud Router: greenearth-router"
    log_info "Cloud NAT: greenearth-nat"
    log_info "Private Google Access: Enabled"
    echo ""
    log_info "You can now create GKE clusters with private nodes"
}

main "$@"
