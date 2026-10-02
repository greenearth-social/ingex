#!/bin/bash

# ingestctl - Simple control script for ingestion services

set -e

# Configuration
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"

SERVICES=(
    "jetstream-ingest-${GE_ENVIRONMENT}"
    "megastream-ingest-${GE_ENVIRONMENT}"
)

SCHEDULED_JOBS=(
    "expiry"
    "extract"
    "followed-users-backfill-targeted"
    "followed-users-backfill-full"
)

COMPUTE_SERVICE_ACCOUNT="21637448064-compute@developer.gserviceaccount.com"

# Check if a name refers to one of the logical scheduled jobs (as opposed to
# a Cloud Run service)
is_scheduled_job() {
    local name=$1
    local job
    for job in "${SCHEDULED_JOBS[@]}"; do
        if [[ "$job" == "$name" ]]; then
            return 0
        fi
    done
    return 1
}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if a service exists
service_exists() {
    local service=$1
    gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1
}

# Get service status (simplified)
get_service_status() {
    local service=$1

    if ! service_exists "$service"; then
        echo "NOT_DEPLOYED"
        return
    fi

    # Check manual instance count (0 = stopped)
    local instance_count=$(gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.metadata.annotations['run.googleapis.com/scaling-mode'])" 2>/dev/null || echo "")

    local manual_count=$(gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.metadata.annotations['run.googleapis.com/manual-scaling'])" 2>/dev/null || echo "")

    if [[ "$manual_count" == "0" ]]; then
        echo "STOPPED"
    elif [[ -n "$manual_count" && "$manual_count" != "0" ]]; then
        echo "RUNNING"
    else
        # Check ready condition
        local ready=$(gcloud run services describe "$service" \
            --region="$GE_GCP_REGION" \
            --project="$GE_GCP_PROJECT_ID" \
            --format="value(status.url)" 2>/dev/null)

        if [[ -n "$ready" ]]; then
            echo "RUNNING"
        else
            echo "STARTING"
        fi
    fi
}

# Start a service
start_service() {
    local service=$1

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Starting $service...${NC}"

    gcloud run services update "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --scaling=1 \
        --quiet

    echo -e "${GREEN}Service $service started (manual scaling: 1 instance)${NC}"
}

# Stop a service
stop_service() {
    local service=$1

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Stopping $service...${NC}"

    gcloud run services update "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --scaling=0 \
        --quiet

    echo -e "${YELLOW}Service $service stopped (manual scaling: 0 instances)${NC}"
}

# Get the Cloud Scheduler job name for a logical job
get_scheduler_job_name() {
    local job=$1
    case "$job" in
        expiry)
            if [ "$GE_ENVIRONMENT" = "prod" ]; then
                echo "elasticsearch-expiry-daily-prod"
            else
                echo "elasticsearch-expiry-halfhourly-${GE_ENVIRONMENT}"
            fi
            ;;
        extract)
            echo "extract-halfhourly-${GE_ENVIRONMENT}"
            ;;
        followed-users-backfill-targeted)
            echo "followed-users-backfill-targeted-${GE_ENVIRONMENT}"
            ;;
        followed-users-backfill-full)
            echo "followed-users-backfill-full-${GE_ENVIRONMENT}"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Get the Cloud Run job name for a logical job. The two followed-users-backfill
# logical jobs are two Cloud Scheduler triggers for the *same* underlying
# Cloud Run job, distinguished only by the --mode arg override at trigger time
# (see get_scheduler_job_args) — mirrors gcp_setup.sh's
# setup_followed_users_backfill_cloud_scheduler.
get_cloudrun_job_name() {
    local job=$1
    case "$job" in
        expiry) echo "elasticsearch-expiry-${GE_ENVIRONMENT}" ;;
        extract) echo "extract-${GE_ENVIRONMENT}" ;;
        followed-users-backfill-targeted|followed-users-backfill-full)
            echo "followed-users-backfill-${GE_ENVIRONMENT}"
            ;;
        *) echo "" ;;
    esac
}

# Get the cron schedule and description for a logical job (pipe-separated).
# Schedules here must stay in sync with gcp_setup.sh's
# setup_followed_users_backfill_cloud_scheduler.
get_scheduler_job_config() {
    local job=$1
    case "$job" in
        expiry)
            if [ "$GE_ENVIRONMENT" = "prod" ]; then
                echo "0 2 * * *|Daily Elasticsearch data expiry for production"
            else
                echo "*/30 * * * *|Half-hourly Elasticsearch data expiry for ${GE_ENVIRONMENT}"
            fi
            ;;
        extract)
            echo "*/30 * * * *|Half hourly extract job for ${GE_ENVIRONMENT}"
            ;;
        followed-users-backfill-targeted)
            if [ "$GE_ENVIRONMENT" = "prod" ]; then
                echo "0 * * * *|Frequent targeted followed-users-backfill sweep (incomplete/invalidated/stale) for ${GE_ENVIRONMENT}"
            else
                echo "*/15 * * * *|Frequent targeted followed-users-backfill sweep (incomplete/invalidated/stale) for ${GE_ENVIRONMENT}"
            fi
            ;;
        followed-users-backfill-full)
            echo "0 3 * * 0|Weekly full followed-users-backfill sweep (missing/pending_overflow backstop) for ${GE_ENVIRONMENT}"
            ;;
    esac
}

# Get the Cloud Run Jobs v2 container args override (JSON array), if any, for
# a logical job. Empty output means no override is needed (the job's
# deploy-time default args apply).
get_scheduler_job_args() {
    local job=$1
    case "$job" in
        followed-users-backfill-targeted)
            echo '["--mode","targeted","--concurrency","20"]'
            ;;
        followed-users-backfill-full)
            echo '["--mode","full","--concurrency","20"]'
            ;;
        *)
            echo ""
            ;;
    esac
}

# Check if a Cloud Scheduler job exists
scheduler_job_exists() {
    local job_name=$1
    gcloud scheduler jobs describe "$job_name" \
        --location="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1
}

# Get scheduler job status for a logical job
get_scheduler_status() {
    local job=$1
    local scheduler_job_name
    scheduler_job_name=$(get_scheduler_job_name "$job")

    if [[ -z "$scheduler_job_name" ]]; then
        echo "UNKNOWN"
        return
    fi

    if scheduler_job_exists "$scheduler_job_name"; then
        echo "ENABLED"
    else
        echo "NOT_DEPLOYED"
    fi
}

# Start (create/update) a scheduled job
start_scheduled_job() {
    local job=$1
    local scheduler_job_name
    local cloudrun_job_name
    scheduler_job_name=$(get_scheduler_job_name "$job")
    cloudrun_job_name=$(get_cloudrun_job_name "$job")

    if [[ -z "$scheduler_job_name" ]]; then
        echo -e "${RED}Error: Unknown job '$job'. Valid jobs: ${SCHEDULED_JOBS[*]}${NC}"
        return 1
    fi

    local config
    config=$(get_scheduler_job_config "$job")
    local schedule="${config%%|*}"
    local description="${config##*|}"
    local job_uri="https://run.googleapis.com/v2/projects/${GE_GCP_PROJECT_ID}/locations/${GE_GCP_REGION}/jobs/${cloudrun_job_name}:run"

    # followed-users-backfill's two logical jobs share one Cloud Run job and
    # are distinguished by a container args override at trigger time (see
    # gcp_setup.sh's create_or_update_backfill_scheduler_job for the same
    # pattern). Other jobs have no override and run with their deploy-time
    # default args.
    local args_json
    args_json=$(get_scheduler_job_args "$job")
    local extra_flags=()
    if [[ -n "$args_json" ]]; then
        local body="{\"overrides\":{\"containerOverrides\":[{\"args\":${args_json}}]}}"
        extra_flags=(--headers="Content-Type=application/json" --message-body="$body")
    fi

    echo -e "${BLUE}Starting scheduled job $scheduler_job_name...${NC}"

    if scheduler_job_exists "$scheduler_job_name"; then
        gcloud scheduler jobs update http "$scheduler_job_name" \
            --location="$GE_GCP_REGION" \
            --project="$GE_GCP_PROJECT_ID" \
            --schedule="$schedule" \
            --uri="$job_uri" \
            --http-method=POST \
            "${extra_flags[@]}" \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description" \
            --quiet
        echo -e "${GREEN}Scheduled job $scheduler_job_name updated (schedule: $schedule)${NC}"
    else
        gcloud scheduler jobs create http "$scheduler_job_name" \
            --location="$GE_GCP_REGION" \
            --project="$GE_GCP_PROJECT_ID" \
            --schedule="$schedule" \
            --uri="$job_uri" \
            --http-method=POST \
            "${extra_flags[@]}" \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description"
        echo -e "${GREEN}Scheduled job $scheduler_job_name created (schedule: $schedule)${NC}"
    fi
}

# Stop (delete) a scheduled job
stop_scheduled_job() {
    local job=$1
    local scheduler_job_name
    scheduler_job_name=$(get_scheduler_job_name "$job")

    if [[ -z "$scheduler_job_name" ]]; then
        echo -e "${RED}Error: Unknown job '$job'. Valid jobs: ${SCHEDULED_JOBS[*]}${NC}"
        return 1
    fi

    if ! scheduler_job_exists "$scheduler_job_name"; then
        echo -e "${YELLOW}Scheduled job $scheduler_job_name is not deployed${NC}"
        return 0
    fi

    echo -e "${BLUE}Stopping scheduled job $scheduler_job_name...${NC}"

    gcloud scheduler jobs delete "$scheduler_job_name" \
        --location="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --quiet

    echo -e "${YELLOW}Scheduled job $scheduler_job_name deleted${NC}"
}

# Show service and scheduled job status
show_status() {
    echo -e "${BLUE}Ingestion Services Status:${NC}"
    echo "========================="

    for service in "${SERVICES[@]}"; do
        local status=$(get_service_status "$service")
        local color

        case "$status" in
            "RUNNING") color="$GREEN" ;;
            "STOPPED") color="$YELLOW" ;;
            "NOT_DEPLOYED") color="$RED" ;;
            *) color="$BLUE" ;;
        esac

        printf "%-40s %b%s%b\n" "$service" "$color" "$status" "$NC"
    done

    echo ""
    echo -e "${BLUE}Scheduled Jobs Status:${NC}"
    echo "======================"

    for job in "${SCHEDULED_JOBS[@]}"; do
        local scheduler_job_name
        scheduler_job_name=$(get_scheduler_job_name "$job")
        local status
        status=$(get_scheduler_status "$job")
        local color

        case "$status" in
            "ENABLED") color="$GREEN" ;;
            "NOT_DEPLOYED") color="$RED" ;;
            *) color="$BLUE" ;;
        esac

        printf "%-40s %b%s%b\n" "$scheduler_job_name" "$color" "$status" "$NC"
    done
}

# Show detailed service information
show_details() {
    local service=$1

    if [[ -z "$service" ]]; then
        echo -e "${RED}Error: Please specify a service name${NC}"
        echo "Available services: ${SERVICES[*]}"
        return 1
    fi

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Details for $service:${NC}"
    echo "===================="

    gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="table(
            metadata.name,
            status.url,
            status.conditions[0].status,
            spec.template.metadata.annotations['autoscaling.knative.dev/minScale']:label=MIN_INSTANCES,
            spec.template.metadata.annotations['autoscaling.knative.dev/maxScale']:label=MAX_INSTANCES
        )"
}

# Main command handling
main() {
    local command=$1
    local target=$2

    case "$command" in
        "start")
            if [[ -n "$target" ]]; then
                if is_scheduled_job "$target"; then
                    start_scheduled_job "$target"
                else
                    start_service "$target"
                fi
            else
                echo -e "${BLUE}Starting all services and scheduled jobs...${NC}"
                for svc in "${SERVICES[@]}"; do
                    start_service "$svc"
                done
                for job in "${SCHEDULED_JOBS[@]}"; do
                    start_scheduled_job "$job"
                done
            fi
            ;;
        "stop")
            if [[ -n "$target" ]]; then
                if is_scheduled_job "$target"; then
                    stop_scheduled_job "$target"
                else
                    stop_service "$target"
                fi
            else
                echo -e "${BLUE}Stopping all services and scheduled jobs...${NC}"
                for svc in "${SERVICES[@]}"; do
                    stop_service "$svc"
                done
                for job in "${SCHEDULED_JOBS[@]}"; do
                    stop_scheduled_job "$job"
                done
            fi
            ;;
        "status")
            show_status
            ;;
        "details")
            show_details "$target"
            ;;
        "restart")
            if [[ -n "$target" ]]; then
                stop_service "$target"
                echo "Waiting 5 seconds..."
                sleep 5
                start_service "$target"
            else
                echo -e "${RED}Error: Please specify a service to restart${NC}"
                return 1
            fi
            ;;
        *)
            echo "Usage: $0 {start|stop|status|details|restart} [target]"
            echo ""
            echo "Commands:"
            echo "  start [target]    - Start service(s) or scheduled job(s)"
            echo "  stop [target]     - Stop service(s) or scheduled job(s)"
            echo "  status            - Show status of all services and scheduled jobs"
            echo "  details <service> - Show detailed info for a service"
            echo "  restart <service> - Restart a specific service"
            echo ""
            echo "Available services: ${SERVICES[*]}"
            echo "Available scheduled jobs: ${SCHEDULED_JOBS[*]}"
            return 1
            ;;
    esac
}

# Run the main function with all arguments
main "$@"
