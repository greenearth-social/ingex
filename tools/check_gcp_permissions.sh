#!/bin/bash

# Print the GCP IAM permissions the current gcloud identity has on the
# project that are relevant to running index/deploy.sh and
# ingest/scripts/deploy.sh. Output is a sorted, one-per-line list of granted
# permissions with no other noise, so two runs (e.g. yours and a teammate's)
# can be diffed directly:
#
#   ./check_gcp_permissions.sh > me.txt
#   ./check_gcp_permissions.sh > teammate.txt   # run by the teammate
#   diff me.txt teammate.txt
#
# There is no `gcloud projects test-iam-permissions` command, so this calls
# the Cloud Resource Manager `testIamPermissions` API directly via curl using
# a gcloud-issued access token.
#
# Usage: ./check_gcp_permissions.sh

set -e

PROJECT=$(gcloud config get-value project 2>/dev/null)
TOKEN=$(gcloud auth print-access-token 2>/dev/null)

PERMISSIONS=$(cat <<'EOF' | paste -sd, -
container.clusters.get
container.clusters.create
container.clusters.getCredentials
container.clusters.list
run.services.create
run.services.update
run.services.get
run.jobs.create
run.jobs.update
run.jobs.run
cloudbuild.builds.create
cloudbuild.builds.get
cloudbuild.builds.list
artifactregistry.repositories.uploadArtifacts
artifactregistry.repositories.downloadArtifacts
artifactregistry.repositories.get
secretmanager.secrets.create
secretmanager.versions.add
secretmanager.versions.access
secretmanager.secrets.get
storage.buckets.get
storage.buckets.create
storage.objects.create
storage.objects.get
storage.objects.list
vpcaccess.connectors.create
vpcaccess.connectors.get
vpcaccess.connectors.use
iam.serviceAccounts.actAs
iam.serviceAccounts.get
iam.serviceAccounts.create
iam.serviceAccounts.setIamPolicy
monitoring.timeSeries.create
monitoring.metricDescriptors.create
serviceusage.services.enable
serviceusage.services.get
EOF
)

json_permissions=$(echo "$PERMISSIONS" | sed 's/,/", "/g')

curl -s -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"permissions\": [\"$json_permissions\"]}" \
    "https://cloudresourcemanager.googleapis.com/v1/projects/${PROJECT}:testIamPermissions" \
    | jq -r '.permissions[]?' | sort
