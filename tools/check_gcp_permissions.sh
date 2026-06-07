#!/bin/bash

# Check whether the current gcloud identity has the IAM permissions needed to
# run index/deploy.sh and ingest/scripts/deploy.sh.
#
# There is no `gcloud projects test-iam-permissions` command, so this calls
# the Cloud Resource Manager `testIamPermissions` API directly via curl using
# a gcloud-issued access token.
#
# Usage: ./check_gcp_permissions.sh

set -e

echo "=== Identity & Project ==="
gcloud auth list
gcloud config get-value project
PROJECT=$(gcloud config get-value project)
TOKEN=$(gcloud auth print-access-token)

# Prints only the permissions from $1 (comma-separated) that the current
# identity actually has on the project; anything missing from the response
# is a permission they lack.
test_permissions() {
    local label="$1"
    local permissions="$2"
    local json_permissions
    json_permissions=$(echo "$permissions" | sed 's/,/", "/g')

    echo ""
    echo "=== $label ==="
    curl -s -X POST \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"permissions\": [\"$json_permissions\"]}" \
        "https://cloudresourcemanager.googleapis.com/v1/projects/${PROJECT}:testIamPermissions"
    echo ""
}

test_permissions "GKE (index/deploy.sh: cluster create/access)" \
  "container.clusters.get,container.clusters.create,container.clusters.getCredentials,container.clusters.list"

test_permissions "Cloud Run (ingest/scripts/deploy.sh: deploy services & jobs)" \
  "run.services.create,run.services.update,run.services.get,run.jobs.create,run.jobs.update,run.jobs.run"

test_permissions "Cloud Build (triggered by 'gcloud run deploy --source=.')" \
  "cloudbuild.builds.create,cloudbuild.builds.get,cloudbuild.builds.list"

test_permissions "Artifact Registry (image push during source-based deploy)" \
  "artifactregistry.repositories.uploadArtifacts,artifactregistry.repositories.downloadArtifacts,artifactregistry.repositories.get"

test_permissions "Secret Manager (env var injection: ES API key, AWS keys)" \
  "secretmanager.secrets.create,secretmanager.versions.add,secretmanager.versions.access,secretmanager.secrets.get"

test_permissions "Cloud Storage (state files, parquet export, blocklist, ES snapshots)" \
  "storage.buckets.get,storage.buckets.create,storage.objects.create,storage.objects.get,storage.objects.list"

test_permissions "VPC Access Connector (used by all Cloud Run services)" \
  "vpcaccess.connectors.create,vpcaccess.connectors.get,vpcaccess.connectors.use"

test_permissions "IAM / Service Accounts (deploy-as ingex-runner-\$ENV)" \
  "iam.serviceAccounts.actAs,iam.serviceAccounts.get,iam.serviceAccounts.create,iam.serviceAccounts.setIamPolicy"

test_permissions "Monitoring (OTel custom metrics export)" \
  "monitoring.timeSeries.create,monitoring.metricDescriptors.create"

test_permissions "Service Usage (enabling APIs - only relevant for fresh setup)" \
  "serviceusage.services.enable,serviceusage.services.get"
