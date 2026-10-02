#!/bin/bash

# backfill_extract.sh - Replay the extract job over a historical time range
#
# The scheduled extract job exports a rolling 33-minute window every 30 minutes.
# When it stops running, the gap has to be filled by re-executing the same Cloud
# Run job with explicit --start-time/--end-time arguments. This script splits a
# range into chunks and drives those executions.
#
# The chunk size exists because the exporter buffers up to GE_PARQUET_MAX_RECORDS
# rows in memory before flushing a file, and separately accumulates every exported
# at_uri for the lifetime of the run. On the prod job's 4Gi that puts a practical
# ceiling on how much wall-clock time a single execution can cover.
#
# Chunk boundaries are inclusive on both ends (the ES range query uses gte/lte),
# so adjacent chunks overlap by an instant rather than leaving a hole. Production
# windows already overlap by 3 minutes, so downstream tolerates duplicates.
#
# Prerequisites: the extract-$GE_ENVIRONMENT Cloud Run job must already be deployed.

set -e

# Configuration
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"  # you can override with --environment

# Defaults
START_TIME=""
END_TIME=""
CHUNK_HOURS=3
PARALLEL=1
DRY_RUN=false
SKIP_INFERENCES=true
STATE_FILE=""

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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

usage() {
    cat <<'EOF'
Usage: backfill_extract.sh --start TIME --end TIME [options]

Required:
  --start TIME          Start of the range to backfill (RFC3339, e.g. 2026-08-05T17:30:00Z)
  --end TIME            End of the range to backfill (RFC3339)

Options:
  --environment ENV     Target environment (default: stage)
  --chunk-hours N       Hours of data per execution (default: 3)
  --parallel N          Concurrent executions (default: 1)
  --with-inferences     Export inferences too. Off by default: the inferences
                        index rolls over daily and is deleted immediately after,
                        so historical inferences no longer exist and exporting
                        them just burns memory accumulating at_uris.
  --state-file PATH     Resume state (default: ./.extract_backfill_state)
  --dry-run             Print the executions that would run, then exit
  --help                Show this message

Examples:
  # Fill the 2026-08-05 -> 2026-08-12 outage in prod, 4 chunks at a time
  ./backfill_extract.sh --environment prod \
      --start 2026-08-05T17:30:00Z --end 2026-08-12T01:27:00Z --parallel 4

  # See the plan without touching anything
  ./backfill_extract.sh --environment prod \
      --start 2026-08-05T17:30:00Z --end 2026-08-12T01:27:00Z --dry-run

A completed chunk is recorded in the state file, so re-running the same command
after an interruption picks up where it left off. Delete the state file to start
over.
EOF
}

# --- portable RFC3339 <-> epoch helpers -------------------------------------
# GNU date and BSD date disagree on both parsing and formatting, and this script
# is run from macOS laptops as often as from Linux. Probe once, then convert
# through epoch seconds so the chunk arithmetic itself is plain integer math.
DATE_FLAVOR=""

detect_date_flavor() {
    if date -u -d "2026-01-01T00:00:00Z" +%s > /dev/null 2>&1; then
        DATE_FLAVOR="gnu"
    elif date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "2026-01-01T00:00:00Z" +%s > /dev/null 2>&1; then
        DATE_FLAVOR="bsd"
    else
        log_error "Could not find a usable 'date' command (tried GNU and BSD syntax)"
        exit 1
    fi
}

# Parse an RFC3339 timestamp into epoch seconds. Prints nothing and returns 1 if
# the input is not a timestamp we can read.
to_epoch() {
    local ts=$1
    if [ "$DATE_FLAVOR" = "gnu" ]; then
        date -u -d "$ts" +%s 2>/dev/null
    else
        date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s 2>/dev/null
    fi
}

# Format epoch seconds back into the RFC3339 form the extract job expects.
from_epoch() {
    local epoch=$1
    if [ "$DATE_FLAVOR" = "gnu" ]; then
        date -u -d "@$epoch" +"%Y-%m-%dT%H:%M:%SZ"
    else
        date -u -j -f "%s" "$epoch" +"%Y-%m-%dT%H:%M:%SZ"
    fi
}

# --- argument parsing --------------------------------------------------------
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --start)
                START_TIME="$2"
                shift 2
                ;;
            --end)
                END_TIME="$2"
                shift 2
                ;;
            --environment)
                GE_ENVIRONMENT="$2"
                shift 2
                ;;
            --chunk-hours)
                CHUNK_HOURS="$2"
                shift 2
                ;;
            --parallel)
                PARALLEL="$2"
                shift 2
                ;;
            --with-inferences)
                SKIP_INFERENCES=false
                shift
                ;;
            --state-file)
                STATE_FILE="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown argument: $1"
                echo
                usage
                exit 1
                ;;
        esac
    done
}

validate_args() {
    if [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
        log_error "--start and --end are both required"
        echo
        usage
        exit 1
    fi

    local start_epoch end_epoch
    start_epoch=$(to_epoch "$START_TIME") || true
    end_epoch=$(to_epoch "$END_TIME") || true

    if [ -z "$start_epoch" ]; then
        log_error "Could not parse --start '$START_TIME'. Expected RFC3339, e.g. 2026-08-05T17:30:00Z"
        exit 1
    fi
    if [ -z "$end_epoch" ]; then
        log_error "Could not parse --end '$END_TIME'. Expected RFC3339, e.g. 2026-08-12T01:27:00Z"
        exit 1
    fi
    if [ "$end_epoch" -le "$start_epoch" ]; then
        log_error "--end must be after --start"
        exit 1
    fi

    if ! [[ "$CHUNK_HOURS" =~ ^[0-9]+$ ]] || [ "$CHUNK_HOURS" -lt 1 ]; then
        log_error "--chunk-hours must be a positive integer"
        exit 1
    fi
    if ! [[ "$PARALLEL" =~ ^[0-9]+$ ]] || [ "$PARALLEL" -lt 1 ]; then
        log_error "--parallel must be a positive integer"
        exit 1
    fi

    if [ -z "$STATE_FILE" ]; then
        STATE_FILE="./.extract_backfill_state"
    fi
}

# --- preflight ---------------------------------------------------------------
# The 2026-08-05 outage was caused by the job being deployed with an empty
# GE_ELASTICSEARCH_URL, which fails fast per execution against 127.0.0.1:9200.
# A backfill against a job in that state would burn hours producing nothing, so
# check the deployed configuration before queueing anything.
preflight() {
    local job_name=$1

    log_step "Checking that job $job_name exists and is configured"

    if ! gcloud run jobs describe "$job_name" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1; then
        log_error "Cloud Run job '$job_name' not found in $GE_GCP_REGION"
        log_error "Deploy it first: ./scripts/deploy.sh extract --environment $GE_ENVIRONMENT"
        exit 1
    fi

    local es_url
    es_url=$(gcloud run jobs describe "$job_name" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.spec.template.spec.containers[0].env.filter(\"name:GE_ELASTICSEARCH_URL\").extract(\"value\"))" 2>/dev/null || echo "")

    if [ -z "$es_url" ]; then
        log_error "$job_name has an empty GE_ELASTICSEARCH_URL"
        log_error "Every execution will fail against 127.0.0.1:9200. Redeploy with GE_ELASTICSEARCH_URL set."
        exit 1
    fi

    log_info "GE_ELASTICSEARCH_URL is set on $job_name"
}

# --- chunk execution ---------------------------------------------------------
# Chunks are keyed in the state file by their exact start/end pair, so a resumed
# run with different --chunk-hours will not falsely match earlier work.
chunk_key() {
    echo "$1|$2"
}

chunk_is_done() {
    local key
    key=$(chunk_key "$1" "$2")
    [ -f "$STATE_FILE" ] && grep -Fqx "$key" "$STATE_FILE"
}

mark_chunk_done() {
    local key
    key=$(chunk_key "$1" "$2")
    echo "$key" >> "$STATE_FILE"
}

build_args() {
    local chunk_start=$1
    local chunk_end=$2
    local args="--start-time,$chunk_start,--end-time,$chunk_end"

    if [ "$SKIP_INFERENCES" = true ]; then
        args="$args,--skip-inferences"
    fi

    echo "$args"
}

# Run one chunk to completion. Writes a per-chunk log so a failure in a parallel
# run can be traced back to its execution rather than interleaved on stdout.
run_chunk() {
    local job_name=$1
    local chunk_start=$2
    local chunk_end=$3
    local index=$4
    local total=$5

    local args
    args=$(build_args "$chunk_start" "$chunk_end")

    local log_file="${LOG_DIR}/chunk_${index}.log"

    log_info "[$index/$total] $chunk_start -> $chunk_end"

    if gcloud run jobs execute "$job_name" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --args="$args" \
        --wait > "$log_file" 2>&1; then
        mark_chunk_done "$chunk_start" "$chunk_end"
        log_info "[$index/$total] completed"
        return 0
    else
        log_error "[$index/$total] FAILED ($chunk_start -> $chunk_end), see $log_file"
        return 1
    fi
}

main() {
    detect_date_flavor
    parse_args "$@"
    validate_args

    local job_name="extract-$GE_ENVIRONMENT"
    local start_epoch end_epoch chunk_seconds
    start_epoch=$(to_epoch "$START_TIME")
    end_epoch=$(to_epoch "$END_TIME")
    chunk_seconds=$((CHUNK_HOURS * 3600))

    local total_chunks
    total_chunks=$(( (end_epoch - start_epoch + chunk_seconds - 1) / chunk_seconds ))

    echo "=================================================="
    echo "Green Earth Ingex - Extract Backfill"
    echo "Environment:  $GE_ENVIRONMENT"
    echo "Job:          $job_name"
    echo "Project:      $GE_GCP_PROJECT_ID"
    echo "Region:       $GE_GCP_REGION"
    echo "Range:        $START_TIME -> $END_TIME"
    echo "Chunk size:   ${CHUNK_HOURS}h ($total_chunks chunks)"
    echo "Parallelism:  $PARALLEL"
    echo "Inferences:   $([ "$SKIP_INFERENCES" = true ] && echo "skipped" || echo "included")"
    echo "State file:   $STATE_FILE"
    echo "=================================================="
    echo

    if [ "$DRY_RUN" = false ]; then
        preflight "$job_name"
        echo
    fi

    # Materialize the chunk list up front so the execution loop below is plain
    # index arithmetic. Bash 3.2 (what macOS ships) has no `wait -n`, so
    # concurrency is done by launching a batch of $PARALLEL and waiting for all
    # of it before starting the next. Chunks take roughly equal time, so a batch
    # barrier costs little against a true sliding window.
    local -a chunk_starts=()
    local -a chunk_ends=()
    local cursor=$start_epoch

    while [ "$cursor" -lt "$end_epoch" ]; do
        local chunk_end_epoch=$((cursor + chunk_seconds))
        if [ "$chunk_end_epoch" -gt "$end_epoch" ]; then
            chunk_end_epoch=$end_epoch
        fi
        chunk_starts+=("$(from_epoch "$cursor")")
        chunk_ends+=("$(from_epoch "$chunk_end_epoch")")
        cursor=$chunk_end_epoch
    done

    if [ "$DRY_RUN" = true ]; then
        local i=0
        while [ "$i" -lt "$total_chunks" ]; do
            echo "gcloud run jobs execute $job_name \\"
            echo "    --region=$GE_GCP_REGION \\"
            echo "    --project=$GE_GCP_PROJECT_ID \\"
            echo "    --args=$(build_args "${chunk_starts[$i]}" "${chunk_ends[$i]}") \\"
            echo "    --wait"
            i=$((i + 1))
        done
        echo
        log_info "Dry run: $total_chunks executions planned, nothing was submitted"
        exit 0
    fi

    LOG_DIR="${TMPDIR:-/tmp}/extract-backfill-$$"
    mkdir -p "$LOG_DIR"

    local skipped=0
    local attempted=0
    local i=0

    while [ "$i" -lt "$total_chunks" ]; do
        local batch_launched=0

        # Fill one batch, skipping chunks a previous run already completed.
        while [ "$i" -lt "$total_chunks" ] && [ "$batch_launched" -lt "$PARALLEL" ]; do
            local chunk_start="${chunk_starts[$i]}"
            local chunk_end="${chunk_ends[$i]}"
            local label=$((i + 1))

            if chunk_is_done "$chunk_start" "$chunk_end"; then
                log_info "[$label/$total_chunks] already done, skipping ($chunk_start -> $chunk_end)"
                skipped=$((skipped + 1))
            else
                attempted=$((attempted + 1))
                # `|| true` keeps a failed chunk from tripping set -e; the state
                # file is what decides success, and it is reconciled below.
                run_chunk "$job_name" "$chunk_start" "$chunk_end" "$label" "$total_chunks" || true &
                batch_launched=$((batch_launched + 1))
            fi

            i=$((i + 1))
        done

        [ "$batch_launched" -gt 0 ] && wait
    done

    # Reconcile against the state file rather than trusting exit codes collected
    # across subshells: a chunk counts as done only if it recorded itself.
    local failed=0
    local -a failed_chunks=()
    local j=0
    while [ "$j" -lt "$total_chunks" ]; do
        if ! chunk_is_done "${chunk_starts[$j]}" "${chunk_ends[$j]}"; then
            failed=$((failed + 1))
            failed_chunks+=("${chunk_starts[$j]} -> ${chunk_ends[$j]}")
        fi
        j=$((j + 1))
    done

    echo
    echo "=================================================="
    log_info "Chunks: $total_chunks total, $skipped already done, $attempted attempted, $failed failed"

    if [ "$failed" -gt 0 ]; then
        log_warn "Logs for this run: $LOG_DIR"
        log_warn "Failed ranges:"
        for chunk in "${failed_chunks[@]}"; do
            echo "  $chunk"
        done
        log_warn "Successful chunks are recorded in $STATE_FILE; re-run the same"
        log_warn "command to retry only what failed."
        exit 1
    fi

    log_info "Backfill complete."
    log_info "Verify with: gcloud storage ls 'gs://$GE_GCP_PROJECT_ID-ingex-extract-$GE_ENVIRONMENT/bsky_posts_*'"
    rm -rf "$LOG_DIR"
}

main "$@"
