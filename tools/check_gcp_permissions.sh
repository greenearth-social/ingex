#!/bin/bash

# Check whether the current gcloud identity has the IAM permissions needed to
# run index/deploy.sh and ingest/scripts/deploy.sh.
# Usage: ./check_gcp_permissions.sh

set -e

echo "=== Identity & Project ==="
gcloud auth list
gcloud config get-value project
PROJECT=$(gcloud config get-value project)

echo ""
echo "=== GKE (index/deploy.sh: cluster create/access) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="container.clusters.get,container.clusters.create,container.clusters.getCredentials,container.clusters.list"

echo ""
echo "=== Cloud Run (ingest/scripts/deploy.sh: deploy services & jobs) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="run.services.create,run.services.update,run.services.get,run.jobs.create,run.jobs.update,run.jobs.run"

echo ""
echo "=== Cloud Build (triggered by 'gcloud run deploy --source=.') ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="cloudbuild.builds.create,cloudbuild.builds.get,cloudbuild.builds.list"

echo ""
echo "=== Artifact Registry (image push during source-based deploy) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="artifactregistry.repositories.uploadArtifacts,artifactregistry.repositories.downloadArtifacts,artifactregistry.repositories.get"

echo ""
echo "=== Secret Manager (env var injection: ES API key, AWS keys) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="secretmanager.secrets.create,secretmanager.versions.add,secretmanager.versions.access,secretmanager.secrets.get"

echo ""
echo "=== Cloud Storage (state files, parquet export, blocklist, ES snapshots) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="storage.buckets.get,storage.buckets.create,storage.objects.create,storage.objects.get,storage.objects.list"

echo ""
echo "=== VPC Access Connector (used by all Cloud Run services) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="vpcaccess.connectors.create,vpcaccess.connectors.get,vpcaccess.connectors.use"

echo ""
echo "=== IAM / Service Accounts (deploy-as ingex-runner-\$ENV) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="iam.serviceAccounts.actAs,iam.serviceAccounts.get,iam.serviceAccounts.create,iam.serviceAccounts.setIamPolicy"

echo ""
echo "=== Monitoring (OTel custom metrics export) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="monitoring.timeSeries.create,monitoring.metricDescriptors.create"

echo ""
echo "=== Service Usage (enabling APIs - only relevant for fresh setup) ==="
gcloud projects test-iam-permissions $PROJECT \
  --permissions="serviceusage.services.enable,serviceusage.services.get"
