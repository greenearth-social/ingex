# Green Earth Network Configuration

## Network Architecture Overview

### VPC and Subnets

```txt
Default VPC Network (us-east1)
├── Subnet: default
│   └── Range: 10.142.0.0/20 (10.142.0.0 - 10.142.15.255)
│
├── GKE Control Planes (Private Clusters)
│   ├── Stage: 172.16.0.0/28 (172.16.0.0 - 172.16.0.15)
│   └── Prod: 172.16.0.16/28 (172.16.0.16 - 172.16.0.31)
│
└── Outbound Internet Access
    ├── Cloud Router: greenearth-router
    └── Cloud NAT: greenearth-nat
        ├── Auto-allocated external IPs
        └── Enables internet access for private nodes
```

### IP Address Allocation

| Resource | CIDR Block | IP Range | Count | Purpose |
|----------|-----------|----------|-------|---------|
| VPC Subnet (default) | 10.142.0.0/20 | 10.142.0.0 - 10.142.15.255 | 4,096 | GKE nodes, pods, services |
| Stage Control Plane | 172.16.0.0/28 | 172.16.0.0 - 172.16.0.15 | 16 | GKE master nodes (stage) |
| Prod Control Plane | 172.16.0.16/28 | 172.16.0.16 - 172.16.0.31 | 16 | GKE master nodes (prod) |

### Private Nodes Architecture

**GKE Cluster Configuration:**

- **Node IPs:** Private only (from 10.142.0.0/20)
- **Control Plane:** Public endpoint, private master CIDR
- **Outbound Internet:** Cloud NAT (for pulling container images)
- **Google APIs:** Private Google Access (GCR, Artifact Registry, GCS)

## Usage

### Setup Network Infrastructure

Run the setup script before creating GKE clusters:

```bash
export GE_GCP_PROJECT_ID="greenearth-471522"
export GE_GCP_REGION="us-east1"

./network/setup.sh
```

The script is idempotent and will:

1. Create Cloud Router (if not exists)
2. Create Cloud NAT configuration (if not exists)
3. Enable Private Google Access on subnet
4. Configure firewall rules for ECK webhook access (if not exists)

## Testing and Validation

### View Current Network Configuration

**Check Cloud Router:**

```bash
gcloud compute routers describe greenearth-router \
  --region=$GE_GCP_REGION \
  --project=$GE_GCP_PROJECT_ID
```

**Check Cloud NAT Status:**

```bash
gcloud compute routers nats describe greenearth-nat \
  --router=greenearth-router \
  --region=$GE_GCP_REGION \
  --project=$GE_GCP_PROJECT_ID
```

**List NAT IP Addresses:**

```bash
gcloud compute routers nats list \
  --router=greenearth-router \
  --region=$GE_GCP_REGION \
  --project=$GE_GCP_PROJECT_ID
```

**View Subnet Configuration:**

```bash
gcloud compute networks subnets describe default \
  --region=$GE_GCP_REGION \
  --project=$GE_GCP_PROJECT_ID \
  --format="get(privateIpGoogleAccess,ipCidrRange)"
```

**List Firewall Rules:**

```bash
gcloud compute firewall-rules list \
  --project=$GE_GCP_PROJECT_ID \
  --filter="name~'eck-webhook'" \
  --format="table(name,network,direction,sourceRanges,allowed)"
```

## Firewall Configuration for ECK

### Why Firewall Rules are Needed

When running GKE clusters with private nodes (`--enable-private-nodes`), GKE creates restrictive firewall rules that only allow traffic from the control plane to worker nodes on ports **443** and **10250**.

However, the **Elastic Cloud on Kubernetes (ECK) operator** requires the Kubernetes API server to reach the ECK webhook validation service on ports **9443** and **8443**. Without proper firewall rules, the following symptoms occur:

- Webhook validation failures preventing resource creation/updates

### Required Ports

| Port | Purpose | Required By |
|------|---------|-------------|
| 9443 | ECK ValidatingWebhook primary port | Kubernetes API server |
| 8443 | ECK ValidatingWebhook fallback port | Kubernetes API server |

### Firewall Rules Created

The `setup.sh` script creates two firewall rules:

1. **allow-stage-master-to-eck-webhook**
   - Source: `172.16.0.0/28` (stage control plane)
   - Destination: Worker nodes
   - Ports: TCP 9443, 8443
   - Purpose: Allow stage API server → ECK webhook

2. **allow-prod-master-to-eck-webhook**
   - Source: `172.16.0.16/28` (prod control plane)
   - Destination: Worker nodes
   - Ports: TCP 9443, 8443
   - Purpose: Allow prod API server → ECK webhook

### Manual Firewall Rule Creation

If you need to create the firewall rules manually for troubleshooting:

**For Stage Environment:**

```bash
gcloud compute firewall-rules create allow-stage-master-to-eck-webhook \
  --project=$GE_GCP_PROJECT_ID \
  --network=default \
  --allow=tcp:9443,tcp:8443 \
  --source-ranges=172.16.0.0/28 \
  --description="Allow stage GKE control plane to reach ECK webhook"
```

**For Prod Environment:**

```bash
gcloud compute firewall-rules create allow-prod-master-to-eck-webhook \
  --project=$GE_GCP_PROJECT_ID \
  --network=default \
  --allow=tcp:9443,tcp:8443 \
  --source-ranges=172.16.0.16/28 \
  --description="Allow prod GKE control plane to reach ECK webhook"
```

### Verification

After creating the firewall rules, verify the ECK operator can proceed:

```bash
# Watch ECK operator logs
kubectl logs -n elastic-system -l control-plane=elastic-operator -f

# Check Elasticsearch cluster status
kubectl get elasticsearch -n greenearth-prod

# Should transition from 'ApplyingChanges' to 'Ready'
```
