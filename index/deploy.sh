#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$SCRIPT_DIR/deploy/k8s"

# set default cluster, region, project id
GKE_REGION="${GKE_REGION:-us-east1}"
GKE_PROJECT_ID="${GKE_PROJECT_ID:-greenearth-471522}"

print_usage() {
    echo "Usage: $0 <environment> [options]"
    echo ""
    echo "Arguments:"
    echo "  environment         Target environment: local, stage, or prod"
    echo ""
    echo "Options:"
    echo "  --install-eck       Install ECK operator before deployment"
    echo "  --skip-templates    Skip template/alias ConfigMaps (for updates)"
    echo "  --dry-run           Show what would be deployed without applying"
    echo "  --no-timeout        Wait indefinitely for resources (no timeout)"
    echo "  --teardown          Delete the entire environment (prompts for confirmation)"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Required Environment Variables:"
    echo "  GE_ELASTICSEARCH_SERVICE_USER_PWD    Password for the Elasticsearch service user"
    echo ""
    echo "Examples:"
    echo "  $0 local                    # Deploy to local environment"
    echo "  $0 stage --install-eck      # Deploy to stage with ECK installation"
    echo "  $0 prod --no-timeout        # Deploy to prod with no timeout"
    echo "  $0 local --teardown         # Delete local environment"
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

cleanup_on_failure() {
    local namespace=$1
    log_error "Deployment failed. Cleaning up resources..."

    kubectl delete -k "$K8S_DIR/environments/$GE_ENVIRONMENT" -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete secret es-service-user-secret -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete job elasticsearch-bootstrap -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete job es-service-user-setup -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete namespace "$namespace"

    log_info "Cleanup completed"
}

wait_for_resource() {
    local resource_type=$1
    local resource_name=$2
    local namespace=$3
    local timeout=${4:-300}
    local start_time=$(date +%s)

    if [ "$NO_TIMEOUT" = true ]; then
        log_info "Waiting for $resource_type/$resource_name to be ready (no timeout)..."
    else
        log_info "Waiting for $resource_type/$resource_name to be ready (timeout: ${timeout}s)..."
    fi

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ "$NO_TIMEOUT" != true ] && [ $elapsed -gt $timeout ]; then
            log_error "Timeout waiting for $resource_type/$resource_name"
            return 1
        fi

        local health=$(kubectl get $resource_type $resource_name -n $namespace -o jsonpath='{.status.health}' 2>/dev/null || echo "")

        # Check readiness based on resource type
        if [ "$resource_type" = "kibana" ]; then
            # Kibana only has health status, no phase field
            if [ "$health" = "green" ]; then
                log_success "$resource_type/$resource_name is ready"
                return 0
            fi
        else
            # Elasticsearch and other resources may have both health and phase
            local phase=$(kubectl get $resource_type $resource_name -n $namespace -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
            # Accept yellow health for non-prod environments (staging with limited nodes)
            if [ "$GE_ENVIRONMENT" != "prod" ] && [ "$health" = "yellow" ] && [ "$phase" = "Ready" ]; then
                log_success "$resource_type/$resource_name is ready (yellow health acceptable for $GE_ENVIRONMENT)"
                return 0
            elif [ "$health" = "green" ] && [ "$phase" = "Ready" ]; then
                log_success "$resource_type/$resource_name is ready"
                return 0
            fi
        fi

        echo -n "."
        sleep 5
    done
}

wait_for_job() {
    local job_name=$1
    local namespace=$2
    local timeout=${3:-300}
    local start_time=$(date +%s)

    if [ "$NO_TIMEOUT" = true ]; then
        log_info "Waiting for job/$job_name to complete (no timeout)..."
    else
        log_info "Waiting for job/$job_name to complete (timeout: ${timeout}s)..."
    fi

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ "$NO_TIMEOUT" != true ] && [ $elapsed -gt $timeout ]; then
            log_error "Timeout waiting for job/$job_name"
            kubectl logs -l job-name=$job_name -n $namespace --tail=50 2>/dev/null || true
            return 1
        fi

        # Check job status using conditions (more reliable than succeeded/failed counters)
        local condition=$(kubectl get job $job_name -n $namespace -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
        local failed_condition=$(kubectl get job $job_name -n $namespace -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")

        if [ "$condition" = "True" ]; then
            log_success "job/$job_name completed successfully"
            return 0
        fi

        if [ "$failed_condition" = "True" ]; then
            log_error "job/$job_name failed"
            kubectl logs -l job-name=$job_name -n $namespace --tail=50 2>/dev/null || true
            return 1
        fi

        echo -n "."
        sleep 5
    done
}

setup_kubectl_context() {
    local environment=$1
    local create_if_missing=$2

    log_info "Setting up kubectl context for $environment environment..."

    if [ "$environment" = "local" ]; then
        if ! command -v minikube &> /dev/null; then
            log_error "minikube is not installed"
            exit 1
        fi

        if ! minikube status &> /dev/null; then
            if [ "$create_if_missing" = true ]; then
                log_info "Starting minikube..."
                minikube start
            else
                log_error "minikube is not running. Start it with: minikube start"
                exit 1
            fi
        fi

        log_info "Setting kubectl context to minikube..."
        kubectl config use-context minikube

    elif [ "$environment" = "stage" ] || [ "$environment" = "prod" ]; then
        if ! command -v gcloud &> /dev/null; then
            log_error "gcloud CLI is not installed"
            exit 1
        fi

        if [ -z "$GE_K8S_CLUSTER" ] || [ -z "$GKE_REGION" ] || [ -z "$GKE_PROJECT_ID" ]; then
            log_error "Required environment variables not set: GE_K8S_CLUSTER, GKE_REGION, GKE_PROJECT_ID"
            exit 1
        fi

        log_info "Checking if GKE cluster exists: $GE_K8S_CLUSTER"
        if ! gcloud container clusters describe "$GE_K8S_CLUSTER" \
            --location="$GKE_REGION" \
            --project="$GKE_PROJECT_ID" &> /dev/null; then

            if [ "$create_if_missing" = true ]; then
                echo ""
                log_warning "GKE cluster $GE_K8S_CLUSTER does not exist in project $GKE_PROJECT_ID"
                log_warning "Region: $GKE_REGION"
                log_warning "This will create a new standard GKE cluster (this may incur costs)"
                echo ""
                read -p "Do you want to create the cluster? Type 'yes' to confirm: " confirmation

                if [ "$confirmation" != "yes" ]; then
                    log_info "Cluster creation cancelled"
                    exit 0
                fi

                if [ "$environment" = "stage" ]; then
                    log_info "Creating standard GKE cluster for stage: $GE_K8S_CLUSTER"
                    gcloud container clusters create "$GE_K8S_CLUSTER" \
                        --location="$GKE_REGION" \
                        --project="$GKE_PROJECT_ID" \
                        --machine-type="n2-highmem-2" \
                        --num-nodes=2 \
                        --disk-size=50 \
                        --disk-type=pd-standard \
                        --enable-autorepair \
                        --enable-autoupgrade \
                        --release-channel=regular \
                        --enable-ip-alias \
                        --enable-private-nodes \
                        --master-ipv4-cidr="172.16.0.0/28" \
                        --no-enable-master-authorized-networks \
                        --no-enable-basic-auth \
                        --no-issue-client-certificate
                    log_success "Stage cluster created successfully"

                elif [ "$environment" = "prod" ]; then
                    log_info "Creating standard GKE cluster for prod: $GE_K8S_CLUSTER"

                    gcloud container clusters create "$GE_K8S_CLUSTER" \
                        --location="$GKE_REGION" \
                        --project="$GKE_PROJECT_ID" \
                        --machine-type="n2-standard-2" \
                        --num-nodes=2 \
                        --node-labels=workload=es-master \
                        --disk-size=50 \
                        --disk-type=pd-standard \
                        --enable-autorepair \
                        --enable-autoupgrade \
                        --release-channel=regular \
                        --enable-ip-alias \
                        --enable-private-nodes \
                        --master-ipv4-cidr="172.16.0.16/28" \
                        --no-enable-master-authorized-networks \
                        --no-enable-basic-auth \
                        --no-issue-client-certificate

                    log_info "Creating data node pool for prod..."
                    gcloud container node-pools create data-pool \
                        --cluster="$GE_K8S_CLUSTER" \
                        --location="$GKE_REGION" \
                        --project="$GKE_PROJECT_ID" \
                        --machine-type="n2-highmem-2" \
                        --num-nodes=4 \
                        --node-labels=workload=es-data \
                        --disk-size=100 \
                        --disk-type=pd-standard \
                        --enable-autorepair \
                        --enable-autoupgrade

                    log_success "Prod cluster with node pools created successfully"
                fi
            else
                log_error "GKE cluster $GE_K8S_CLUSTER does not exist in project $GKE_PROJECT_ID"
                exit 1
            fi
        else
            log_info "GKE cluster exists"
        fi

        log_info "Getting credentials for GKE cluster..."
        gcloud container clusters get-credentials "$GE_K8S_CLUSTER" \
            --location="$GKE_REGION" \
            --project="$GKE_PROJECT_ID"
    fi

    local current_context=$(kubectl config current-context)
    log_success "kubectl context set to: $current_context"
}

verify_prerequisites() {
    log_info "Verifying prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi

    if [ -z "$GE_ELASTICSEARCH_SERVICE_USER_PWD" ]; then
        log_error "GE_ELASTICSEARCH_SERVICE_USER_PWD environment variable is not set"
        exit 1
    fi

    log_success "Prerequisites verified"
}

install_eck_operator() {
    log_info "Installing ECK operator..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would install ECK CRDs and operator"
        return 0
    fi

    kubectl create -f https://download.elastic.co/downloads/eck/3.1.0/crds.yaml 2>/dev/null || log_warning "ECK CRDs may already exist"
    kubectl apply -f https://download.elastic.co/downloads/eck/3.1.0/operator.yaml

    log_info "Waiting for ECK operator to be ready..."
    kubectl wait --for=condition=Ready pod -n elastic-system -l control-plane=elastic-operator --timeout=300s

    log_success "ECK operator installed"
}

teardown_environment() {
    local namespace=$1

    echo ""
    log_warning "WARNING: This will DELETE the entire $GE_ENVIRONMENT environment!"
    log_warning "Namespace: $namespace"
    log_warning "All data will be permanently lost."
    echo ""
    read -p "Are you sure you want to proceed? Type 'yes' to confirm: " confirmation

    if [ "$confirmation" != "yes" ]; then
        log_info "Teardown cancelled"
        exit 0
    fi

    log_info "Tearing down $GE_ENVIRONMENT environment..."

    kubectl delete namespace $namespace --ignore-not-found=true

    log_success "Environment $GE_ENVIRONMENT has been deleted"
}

deploy_environment() {
    local environment=$1
    local namespace="greenearth-$environment"

    GE_K8S_CLUSTER="${GE_K8S_CLUSTER:-greenearth-$environment-cluster}"

    if [ "$TEARDOWN" = true ]; then
        setup_kubectl_context "$environment" false
        teardown_environment "$namespace"
        exit 0
    fi

    setup_kubectl_context "$environment" true

    log_info "Deploying to $environment environment (namespace: $namespace)"

    trap "cleanup_on_failure $namespace" ERR

    if [ "$INSTALL_ECK" = true ]; then
        install_eck_operator
    fi

    log_info "Creating namespace $namespace..."
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would create namespace $namespace"
    else
        kubectl create namespace $namespace 2>/dev/null || log_info "Namespace $namespace already exists"
    fi

    if [ "$environment" = "stage" ] || [ "$environment" = "prod" ]; then
        log_info "Deploying DaemonSet for vm.max_map_count..."
        if [ "$DRY_RUN" = true ]; then
            log_info "[DRY RUN] Would deploy max-map-count-daemonset"
        else
            kubectl apply -f "$K8S_DIR/environments/$environment/max-map-count-daemonset.yaml"
            log_info "Waiting 30 seconds for DaemonSet to initialize..."
            sleep 30
        fi
    fi

    log_info "Applying Kustomize manifests..."
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would apply:"
        kubectl kustomize "$K8S_DIR/environments/$environment"
    else
        kubectl apply -k "$K8S_DIR/environments/$environment"
    fi

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Deployment would continue with resource waiting and verification"
        exit 0
    fi

    wait_for_resource "elasticsearch" "greenearth" "$namespace" 600 || {
        log_error "Elasticsearch failed to become ready"
        exit 1
    }

    wait_for_resource "kibana" "greenearth" "$namespace" 300 || {
        log_error "Kibana failed to become ready"
        exit 1
    }

    log_info "Creating service user secret..."
    kubectl create secret generic es-service-user-secret \
        --from-literal=username="es-service-user" \
        --from-literal=password="$GE_ELASTICSEARCH_SERVICE_USER_PWD" \
        -n "$namespace" \
        --dry-run=client -o yaml | kubectl apply -f -

    log_info "Deploying service user setup job..."
    kubectl apply -f "$K8S_DIR/base/es-service-user-setup-job.yaml" -n "$namespace"

    log_info "Waiting for service user setup job..."
    wait_for_job "es-service-user-setup" "$namespace" 180 || {
        log_error "Service user setup job failed"
        exit 1
    }

    log_info "Deploying bootstrap job..."
    kubectl apply -f "$K8S_DIR/base/bootstrap-job.yaml" -n "$namespace"

    log_info "Waiting for bootstrap job..."
    wait_for_job "elasticsearch-bootstrap" "$namespace" 180 || {
        log_error "Bootstrap job failed"
        exit 1
    }

    log_success "Deployment to $environment completed successfully!"
    echo ""
    log_info "Next steps:"
    log_info "  - Access Kibana: kubectl port-forward service/greenearth-kb-http 5601 -n $namespace"
    log_info "  - Access Elasticsearch: kubectl port-forward service/greenearth-es-http 9200 -n $namespace"
    log_info "  - Get elastic password: kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $namespace"
}

GE_ENVIRONMENT=""
INSTALL_ECK=false
SKIP_TEMPLATES=false
DRY_RUN=false
NO_TIMEOUT=false
TEARDOWN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        local|stage|prod)
            GE_ENVIRONMENT="$1"
            shift
            ;;
        --install-eck)
            INSTALL_ECK=true
            shift
            ;;
        --skip-templates)
            SKIP_TEMPLATES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-timeout)
            NO_TIMEOUT=true
            shift
            ;;
        --teardown)
            TEARDOWN=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

if [ -z "$GE_ENVIRONMENT" ]; then
    log_error "Environment argument is required"
    print_usage
    exit 1
fi

if [ "$GE_ENVIRONMENT" != "local" ] && [ "$GE_ENVIRONMENT" != "stage" ] && [ "$GE_ENVIRONMENT" != "prod" ]; then
    log_error "Invalid environment: $GE_ENVIRONMENT (must be local, stage, or prod)"
    exit 1
fi

if [ "$TEARDOWN" != true ]; then
    verify_prerequisites
fi

deploy_environment "$GE_ENVIRONMENT"
