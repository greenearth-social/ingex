#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="greenearth-471522"

gcloud monitoring dashboards create \
  --config-from-file="${SCRIPT_DIR}/minilm-knn-dashboard.json" \
  --project="${PROJECT_ID}"
