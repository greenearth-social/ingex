#!/bin/bash

# Green Earth Ingex - Cloud Run rollback
#
# Rolls the ingest workloads back to a previously deployed state. All four are
# deployed from source (`--source=.`), so the repo never names an image tag and
# there is nothing to rebuild from: the deployed artifacts themselves are the
# rollback targets (see greenearth-social/api#181).
#
# Two workload kinds, two mechanisms:
#
#   services (jetstream-ingest, megastream-ingest)
#       Each deploy leaves a revision pinning the built image digest and the env
#       config it ran with. Rolling back shifts 100% of traffic to an older one.
#       deploy.sh keeps the 10 most recent revisions — that is the window.
#
#   jobs (elasticsearch-expiry, extract)
#       Cloud Run jobs have no revisions; a job is a single mutable template.
#       Their history lives in executions, each of which snapshots the image
#       digest and GE_GIT_SHA it ran with. Rolling back re-points the job's
#       template at an earlier execution's image. Artifact Registry keeps old
#       digests, and both jobs run on a schedule, so recent deployments are
#       recoverable.
#
# Rollbacks are manual. Cloud Run's own health-check behavior is untouched: a
# service revision that never becomes Ready never receives traffic.
#
# After a service rollback, traffic is pinned to a named revision. The next
# successful deploy.sh run resets traffic to LATEST, so "deploy the fix" is also
# how you leave the rolled-back state. For jobs, the next `jobs deploy` replaces
# the template outright, which has the same effect.

set -e

# Configuration
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"

# Rollback target: a revision/execution name, or a git sha. Empty means "the
# previous deployment", resolved per workload.
TARGET=""

LIST_ONLY=false
DRY_RUN=false
ASSUME_YES=false

# How long to wait for a rolled-back service's /health to report the target sha.
HEALTH_TIMEOUT_SEC=90

# How far back to read a job's execution history when building its list of
# deployed generations. Jobs run every 30 minutes and Cloud Run retains on the
# order of a thousand executions, so this bounds the job rollback window: 1000
# executions reaches back roughly three weeks, which spans several deploys.
# Raise it with --max-executions to reach further back, at the cost of a slower
# listing.
MAX_EXECUTIONS=1000

# Set when a target's baked-in Elasticsearch address no longer matches the live
# internal load balancer.
STALE_ELASTICSEARCH_ADDRESS=false

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

log_action() {
    echo -e "${BLUE}[ROLLBACK]${NC} $1"
}

# --- workload naming -------------------------------------------------------

service_workloads() {
    echo "jetstream-ingest-$GE_ENVIRONMENT"
    echo "megastream-ingest-$GE_ENVIRONMENT"
}

job_workloads() {
    echo "elasticsearch-expiry-$GE_ENVIRONMENT"
    echo "extract-$GE_ENVIRONMENT"
}

# --- service helpers -------------------------------------------------------

serving_revision() {
    local service="$1"
    gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(status.traffic.revisionName)" | head -n 1
}

revision_git_sha() {
    local revision="$1"
    gcloud run revisions describe "$revision" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(metadata.labels.git-sha)" 2>/dev/null || true
}

revision_env_var() {
    local revision="$1"
    local var_name="$2"
    gcloud run revisions describe "$revision" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --flatten="spec.containers[].env[]" \
        --format="value(spec.containers.env.name,spec.containers.env.value)" 2>/dev/null \
        | awk -F'\t' -v name="$var_name" '$1 == name { print $2; exit }'
}

# Ready revisions, newest first, as "name|git-sha|created". Pipe-separated
# rather than tab-separated because bash collapses runs of whitespace
# delimiters: an unlabelled revision would otherwise shift its timestamp into
# the sha field.
ready_revisions() {
    local service="$1"
    gcloud run revisions list \
        --service="$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --filter="status.conditions.type=Ready AND status.conditions.status=True" \
        --sort-by="~metadata.creationTimestamp" \
        --format="value[separator='|'](metadata.name,metadata.labels.git-sha,metadata.creationTimestamp)"
}

# --- job helpers -----------------------------------------------------------

job_image() {
    local job="$1"
    gcloud run jobs describe "$job" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.spec.template.spec.containers[0].image)" 2>/dev/null || true
}

job_git_sha() {
    local job="$1"
    gcloud run jobs describe "$job" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(metadata.labels.git-sha)" 2>/dev/null || true
}

execution_env_var() {
    local execution="$1"
    local var_name="$2"
    gcloud run jobs executions describe "$execution" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --flatten="spec.template.spec.containers[].env[]" \
        --format="value(spec.template.spec.containers.env.name,spec.template.spec.containers.env.value)" 2>/dev/null \
        | awk -F'\t' -v name="$var_name" '$1 == name { print $2; exit }'
}

# Executions newest first, as "name|git-sha|created|image". The list response
# already carries each execution's resolved image digest, so this is one API
# call rather than a describe per execution — worth caring about, since the jobs
# run every 30 minutes and the history runs to hundreds of executions.
job_executions() {
    local job="$1"
    gcloud run jobs executions list \
        --job="$job" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --sort-by="~metadata.creationTimestamp" \
        --limit="$MAX_EXECUTIONS" \
        --format="value[separator='|'](metadata.name,metadata.labels.git-sha,metadata.creationTimestamp,spec.template.spec.containers[0].image)"
}

# One line per distinct deployed generation, newest first, as
# "execution|git-sha|created|image". Both jobs run on a schedule, so the raw
# execution list is mostly repeats of the same image; collapsing by digest turns
# it into a deployment history.
job_generations() {
    local job="$1"
    local seen_images=""

    while IFS='|' read -r execution sha created image; do
        [ -z "$execution" ] && continue
        [ -z "$image" ] && continue

        case "$seen_images" in
            *"|$image|"*) continue ;;
        esac
        seen_images="$seen_images|$image|"

        echo "$execution|$sha|$created|$image"
    done <<< "$(job_executions "$job")"
}

# --- listing ---------------------------------------------------------------

list_service() {
    local service="$1"
    local serving
    serving="$(serving_revision "$service")"

    echo ""
    log_info "$service (service — traffic-based rollback, last 10 revisions kept)"
    printf "    %-36s %-10s %s\n" "REVISION" "GIT SHA" "CREATED"

    while IFS='|' read -r revision sha created; do
        [ -z "$revision" ] && continue
        local marker="  "
        if [ "$revision" = "$serving" ]; then
            marker="=>"
        fi
        printf "%s  %-36s %-10s %s\n" "$marker" "$revision" "${sha:-(unstamped)}" "$created"
    done <<< "$(ready_revisions "$service")"
}

list_job() {
    local job="$1"
    local current_image current_sha
    current_image="$(job_image "$job")"
    current_sha="$(job_git_sha "$job")"

    echo ""
    log_info "$job (job — image-based rollback; currently ${current_sha:-unstamped})"
    printf "    %-32s %-10s %-30s %s\n" "EXECUTION" "GIT SHA" "LAST RUN" "IMAGE DIGEST"

    while IFS='|' read -r execution sha created image; do
        [ -z "$execution" ] && continue
        local marker="  "
        if [ "$image" = "$current_image" ]; then
            marker="=>"
        fi
        printf "%s  %-32s %-10s %-30s %s\n" \
            "$marker" "$execution" "${sha:-(unstamped)}" "$created" "${image##*@}"
    done <<< "$(job_generations "$job")"
}

list_all() {
    log_info "Rollback candidates in $GE_ENVIRONMENT (newest first):"

    local workload
    for workload in $(selected_services); do
        list_service "$workload"
    done
    for workload in $(selected_jobs); do
        list_job "$workload"
    done

    echo ""
    echo "  => currently deployed"
    echo ""
    echo "  Roll everything back one deployment:  $0 --environment $GE_ENVIRONMENT"
    echo "  Roll back one workload:               $0 jetstream --environment $GE_ENVIRONMENT"
    echo "  Roll back to a specific target:       $0 --environment $GE_ENVIRONMENT --to <git-sha>"
}

# --- target resolution -----------------------------------------------------

# Newest Ready revision older than the serving one with a different git sha.
resolve_previous_revision() {
    local service="$1"
    local serving="$2"
    local serving_sha="$3"
    local seen_serving=false

    while IFS='|' read -r revision sha _created; do
        [ -z "$revision" ] && continue

        if [ "$revision" = "$serving" ]; then
            seen_serving=true
            continue
        fi

        # The list is newest-first, so anything before the serving entry is newer.
        [ "$seen_serving" = false ] && continue

        if [ -z "$sha" ] || [ -z "$serving_sha" ] || [ "$sha" != "$serving_sha" ]; then
            echo "$revision"
            return 0
        fi
    done <<< "$(ready_revisions "$service")"

    return 1
}

# Accepts a revision name or a git sha; echoes the resolved revision name.
resolve_service_target() {
    local service="$1"
    local requested="$2"

    if gcloud run revisions describe "$requested" \
        --region="$GE_GCP_REGION" --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1; then
        echo "$requested"
        return 0
    fi

    local match
    match="$(ready_revisions "$service" | awk -F'[|]' -v sha="$requested" '$2 == sha { print $1; exit }')"

    if [ -n "$match" ]; then
        echo "$match"
        return 0
    fi

    return 1
}

# Echoes "execution|git-sha|image" for the most recent generation whose image
# differs from what the job runs now.
resolve_previous_generation() {
    local job="$1"
    local current_image="$2"

    while IFS='|' read -r execution sha _created image; do
        [ -z "$execution" ] && continue
        if [ "$image" != "$current_image" ]; then
            echo "$execution|$sha|$image"
            return 0
        fi
    done <<< "$(job_generations "$job")"

    return 1
}

# Accepts an execution name, a git sha, or an image digest.
resolve_job_target() {
    local job="$1"
    local requested="$2"

    while IFS='|' read -r execution sha _created image; do
        [ -z "$execution" ] && continue
        if [ "$execution" = "$requested" ] || [ "$sha" = "$requested" ] || [ "${image##*@}" = "$requested" ]; then
            echo "$execution|$sha|$image"
            return 0
        fi
    done <<< "$(job_generations "$job")"

    return 1
}

# --- safety checks ---------------------------------------------------------

live_elasticsearch_url() {
    if ! command -v kubectl &> /dev/null; then
        return 1
    fi

    local lb_ip
    lb_ip=$(kubectl get service greenearth-es-internal-lb \
        -n "greenearth-$GE_ENVIRONMENT" \
        -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")

    if [ -z "$lb_ip" ] || [ "$lb_ip" = "null" ]; then
        return 1
    fi

    echo "https://$lb_ip:9200"
}

# Every ingest workload reaches Elasticsearch at an internal load balancer IP
# that deploy.sh bakes in at deploy time. If that IP has moved since the target
# was deployed, rolling back would restore a dead address — redeploying the old
# git sha is the right move instead. Best effort: no kubectl, no check.
check_elasticsearch_address() {
    local workload="$1"
    local target_url="$2"

    [ -z "$target_url" ] && return 0

    local current_url
    if ! current_url="$(live_elasticsearch_url)"; then
        log_warn "$workload: could not read the live Elasticsearch LB IP — skipping address check."
        log_warn "$workload: target points at $target_url; confirm that is still current."
        return 0
    fi

    if [ "$target_url" != "$current_url" ]; then
        log_warn "$workload: target's Elasticsearch address looks stale:"
        log_warn "  target:  $target_url"
        log_warn "  current: $current_url"
        STALE_ELASTICSEARCH_ADDRESS=true
    fi
}

confirm() {
    echo ""
    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi

    local prompt="Apply the rollback shown above? Type 'yes' to confirm: "
    if [ "$STALE_ELASTICSEARCH_ADDRESS" = true ]; then
        log_warn "One or more targets carry a stale Elasticsearch address. Rolling back"
        log_warn "would restore it. If Elasticsearch has moved, redeploy the old sha instead:"
        log_warn "  git checkout <sha> && ./scripts/deploy.sh --environment $GE_ENVIRONMENT"
        prompt="Roll back anyway? Type 'yes' to confirm: "
    fi

    local reply
    read -r -p "$prompt" reply
    if [ "$reply" != "yes" ]; then
        log_info "Aborted — nothing changed."
        exit 0
    fi
}

# --- rollback --------------------------------------------------------------

rollback_service() {
    local service="$1"
    local target="$2"

    local cmd="gcloud run services update-traffic $service"
    cmd="$cmd --region=$GE_GCP_REGION"
    cmd="$cmd --project=$GE_GCP_PROJECT_ID"
    cmd="$cmd --to-revisions=$target=100"

    if [ "$DRY_RUN" = true ]; then
        echo "  $cmd"
        return 0
    fi

    log_action "$service -> $target"
    if ! eval "$cmd" > /dev/null; then
        log_error "Failed to shift traffic for $service"
        exit 1
    fi
}

# Jobs have no traffic split, so rolling one back rewrites its template to run
# the older image. GE_GIT_SHA and the git-sha label are restamped so the job
# keeps reporting truthfully what it runs. Note this rolls back the image only —
# other env vars keep their current values.
rollback_job() {
    local job="$1"
    local image="$2"
    local sha="$3"

    local cmd="gcloud run jobs update $job"
    cmd="$cmd --region=$GE_GCP_REGION"
    cmd="$cmd --project=$GE_GCP_PROJECT_ID"
    cmd="$cmd --image=$image"
    if [ -n "$sha" ]; then
        cmd="$cmd --update-env-vars=GE_GIT_SHA=$sha"
        cmd="$cmd --update-labels=git-sha=$sha"
    fi

    if [ "$DRY_RUN" = true ]; then
        echo "  $cmd"
        return 0
    fi

    log_action "$job -> ${image##*@} (${sha:-unstamped})"
    if ! eval "$cmd" > /dev/null; then
        log_error "Failed to update job $job"
        exit 1
    fi
}

# Confirms a service rollback took effect from outside Cloud Run's own
# bookkeeping, by asking the running service which git sha it is.
verify_service_health() {
    local service="$1"
    local target_sha="$2"

    if [ -z "$target_sha" ]; then
        log_warn "$service: target has no git-sha label — skipping /health verification."
        return 0
    fi

    local service_url
    service_url=$(gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" --project="$GE_GCP_PROJECT_ID" --format="value(status.url)")

    log_info "$service: verifying /health reports git sha $target_sha..."

    local deadline=$((SECONDS + HEALTH_TIMEOUT_SEC))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local reported
        reported=$(curl -fsS --max-time 10 "$service_url/health" 2>/dev/null \
            | sed -n 's/.*"git_sha"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

        if [ "$reported" = "$target_sha" ]; then
            log_info "✓ $service reports git sha $reported"
            return 0
        fi

        sleep 3
    done

    log_warn "$service: /health did not report git sha $target_sha within ${HEALTH_TIMEOUT_SEC}s."
    log_warn "$service: traffic was shifted; check manually before assuming the rollback failed."
    return 0
}

# --- workload selection ----------------------------------------------------

selected_services() {
    case "$WORKLOAD" in
        all) service_workloads ;;
        jetstream|jetstream-ingest) echo "jetstream-ingest-$GE_ENVIRONMENT" ;;
        megastream|megastream-ingest) echo "megastream-ingest-$GE_ENVIRONMENT" ;;
        *) ;;
    esac
}

selected_jobs() {
    case "$WORKLOAD" in
        all) job_workloads ;;
        expiry|elasticsearch-expiry) echo "elasticsearch-expiry-$GE_ENVIRONMENT" ;;
        extract|extract-job) echo "extract-$GE_ENVIRONMENT" ;;
        *) ;;
    esac
}

# --- main ------------------------------------------------------------------

# Rollback plans, resolved before anything is changed so the confirmation prompt
# can show the whole picture and a resolution failure aborts cleanly.
SERVICE_PLAN=""   # lines of "service|target-revision|target-sha"
JOB_PLAN=""       # lines of "job|image|target-sha"

plan_services() {
    local service
    for service in $(selected_services); do
        local serving serving_sha target target_sha
        serving="$(serving_revision "$service")"
        if [ -z "$serving" ]; then
            log_error "$service: could not determine which revision is serving traffic."
            exit 1
        fi
        serving_sha="$(revision_git_sha "$serving")"

        if [ -n "$TARGET" ]; then
            if ! target="$(resolve_service_target "$service" "$TARGET")"; then
                log_error "$service: no revision matches '$TARGET' (tried revision name, then git sha)."
                log_error "Run '$0 $WORKLOAD --environment $GE_ENVIRONMENT --list' to see candidates."
                exit 1
            fi
        else
            if ! target="$(resolve_previous_revision "$service" "$serving" "$serving_sha")"; then
                log_error "$service: no previous Ready revision found to roll back to."
                exit 1
            fi
        fi

        if [ "$target" = "$serving" ]; then
            log_info "$service: $target is already serving traffic — skipping."
            continue
        fi

        target_sha="$(revision_git_sha "$target")"

        echo "  $service"
        echo "    serving: $serving (${serving_sha:-unstamped})"
        echo "    target:  $target (${target_sha:-unstamped})"

        check_elasticsearch_address "$service" "$(revision_env_var "$target" GE_ELASTICSEARCH_URL)"

        SERVICE_PLAN="$SERVICE_PLAN$service|$target|$target_sha"$'\n'
    done
}

plan_jobs() {
    local job
    for job in $(selected_jobs); do
        local current_image current_sha generation execution target_sha image
        current_image="$(job_image "$job")"
        if [ -z "$current_image" ]; then
            log_error "$job: could not read the job's current image."
            exit 1
        fi
        current_sha="$(job_git_sha "$job")"

        if [ -n "$TARGET" ]; then
            if ! generation="$(resolve_job_target "$job" "$TARGET")"; then
                log_error "$job: no execution matches '$TARGET' (tried execution name, git sha, image digest)."
                log_error "Run '$0 $WORKLOAD --environment $GE_ENVIRONMENT --list' to see candidates."
                exit 1
            fi
        else
            if ! generation="$(resolve_previous_generation "$job" "$current_image")"; then
                log_error "$job: no earlier image found in the job's execution history."
                log_error "Only images that actually ran are recoverable; redeploy an older sha instead."
                exit 1
            fi
        fi

        IFS='|' read -r execution target_sha image <<< "$generation"

        if [ "$image" = "$current_image" ]; then
            log_info "$job: already running that image — skipping."
            continue
        fi

        echo "  $job"
        echo "    current: ${current_image##*@} (${current_sha:-unstamped})"
        echo "    target:  ${image##*@} (${target_sha:-unstamped}, last ran as $execution)"

        check_elasticsearch_address "$job" "$(execution_env_var "$execution" GE_ELASTICSEARCH_URL)"

        JOB_PLAN="$JOB_PLAN$job|$image|$target_sha"$'\n'
    done
}

apply_plan() {
    if [ "$DRY_RUN" = true ]; then
        log_info "[dry run] would execute:"
    fi

    local line
    while IFS='|' read -r service target _target_sha; do
        [ -z "$service" ] && continue
        rollback_service "$service" "$target"
    done <<< "$SERVICE_PLAN"

    while IFS='|' read -r job image target_sha; do
        [ -z "$job" ] && continue
        rollback_job "$job" "$image" "$target_sha"
    done <<< "$JOB_PLAN"

    if [ "$DRY_RUN" = true ]; then
        log_info "[dry run] no changes made."
        exit 0
    fi
}

verify_plan() {
    while IFS='|' read -r service _target target_sha; do
        [ -z "$service" ] && continue
        verify_service_health "$service" "$target_sha"
    done <<< "$SERVICE_PLAN"

    local job_count
    job_count="$(echo -n "$JOB_PLAN" | grep -c . || true)"
    if [ "$job_count" -gt 0 ]; then
        echo ""
        log_info "Jobs run on a schedule — the rolled-back image takes effect on the next run."
        log_info "To prove it now, trigger one:"
        while IFS='|' read -r job _image _sha; do
            [ -z "$job" ] && continue
            echo "  gcloud run jobs execute $job --region=$GE_GCP_REGION"
        done <<< "$JOB_PLAN"
    fi
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - Cloud Run Rollback"
    echo "Environment: $GE_ENVIRONMENT"
    echo "Workload:    $WORKLOAD"
    echo "Project:     $GE_GCP_PROJECT_ID"
    echo "Region:      $GE_GCP_REGION"
    echo "=================================================="

    if [ "$LIST_ONLY" = true ]; then
        list_all
        exit 0
    fi

    echo ""
    log_info "Planned rollback:"
    echo ""
    plan_services
    plan_jobs

    if [ -z "$SERVICE_PLAN" ] && [ -z "$JOB_PLAN" ]; then
        log_info "Nothing to roll back."
        exit 0
    fi

    confirm
    apply_plan
    verify_plan

    echo ""
    log_info "To leave the rolled-back state, deploy the fix normally:"
    echo "  ./scripts/deploy.sh --environment $GE_ENVIRONMENT"
    echo "  (services reset to LATEST on success; a job deploy replaces its template)"
}

WORKLOAD="all"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-id)
            GE_GCP_PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            GE_GCP_REGION="$2"
            shift 2
            ;;
        --environment)
            GE_ENVIRONMENT="$2"
            shift 2
            ;;
        --to)
            TARGET="$2"
            shift 2
            ;;
        --max-executions)
            MAX_EXECUTIONS="$2"
            shift 2
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --yes)
            ASSUME_YES=true
            shift
            ;;
        --help)
            echo "Usage: $0 [WORKLOAD] [OPTIONS]"
            echo
            echo "Rolls ingest workloads back to a previously deployed state. Services"
            echo "roll back by shifting traffic to an older revision; jobs roll back by"
            echo "re-pointing their template at an earlier execution's image."
            echo
            echo "Workloads (same names as deploy.sh):"
            echo "  jetstream                   jetstream-ingest service only"
            echo "  megastream                  megastream-ingest service only"
            echo "  expiry                      elasticsearch-expiry job only"
            echo "  extract                     extract job only"
            echo "  all                         All four (default)"
            echo
            echo "Options:"
            echo "  --environment ENV           Environment name (default: stage)"
            echo "  --to TARGET                 Roll back to a specific git sha, revision"
            echo "                              name, or (jobs) execution name / image digest"
            echo "  --list                      List rollback candidates and exit"
            echo "  --max-executions N          How far back to read job execution history"
            echo "                              (default: 1000, roughly three weeks)"
            echo "  --dry-run                   Show what would change, execute nothing"
            echo "  --yes                       Skip the confirmation prompt"
            echo "  --project-id ID             GCP project ID (default: greenearth-471522)"
            echo "  --region REGION             GCP region (default: us-east1)"
            echo "  --help                      Show this help message"
            echo
            echo "Environment variables:"
            echo "  GE_ENVIRONMENT              Same as --environment"
            echo "  GE_GCP_PROJECT_ID           Same as --project-id"
            echo "  GE_GCP_REGION               Same as --region"
            echo
            echo "Examples:"
            echo "  $0 --environment prod --list       Show candidates for all workloads"
            echo "  $0 --environment prod              Roll everything back one deployment"
            echo "  $0 jetstream --environment prod    Roll back jetstream-ingest only"
            echo "  $0 --environment prod --to e11d1b2 Roll everything back to a git sha"
            echo
            echo "Notes:"
            echo "  - deploy.sh keeps the 10 most recent service revisions; that is the"
            echo "    rollback window. Jobs are limited by execution history instead."
            echo "  - A job's rollback window is bounded by its execution history;"
            echo "    see --max-executions. Only images that actually ran are recoverable."
            echo "  - Rolling a job back changes its image only; other env vars keep"
            echo "    their current values."
            exit 0
            ;;
        jetstream|jetstream-ingest|megastream|megastream-ingest|expiry|elasticsearch-expiry|extract|extract-job|all)
            WORKLOAD="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

main
