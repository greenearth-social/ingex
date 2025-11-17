# Green Earth Network Configuration

## Network Architecture Overview

### VPC and Subnets

```
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
export GKE_PROJECT_ID="greenearth-471522"
export GKE_REGION="us-east1"

./network/setup.sh
```

The script is idempotent and will:
1. Create Cloud Router (if not exists)
2. Create Cloud NAT configuration (if not exists)
3. Enable Private Google Access on subnet

## Testing and Validation

### View Current Network Configuration

**Check Cloud Router:**
```bash
gcloud compute routers describe greenearth-router \
  --region=$GKE_REGION \
  --project=$GKE_PROJECT_ID
```

**Check Cloud NAT Status:**
```bash
gcloud compute routers nats describe greenearth-nat \
  --router=greenearth-router \
  --region=$GKE_REGION \
  --project=$GKE_PROJECT_ID
```

**List NAT IP Addresses:**
```bash
gcloud compute routers nats list \
  --router=greenearth-router \
  --region=$GKE_REGION \
  --project=$GKE_PROJECT_ID
```

**View Subnet Configuration:**
```bash
gcloud compute networks subnets describe default \
  --region=$GKE_REGION \
  --project=$GKE_PROJECT_ID \
  --format="get(privateIpGoogleAccess,ipCidrRange)"
```
