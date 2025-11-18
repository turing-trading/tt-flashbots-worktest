# Deployment Guide - Flashbots Live Streaming

This directory contains Docker and Kubernetes configurations for deploying the Flashbots live streaming application.

## Prerequisites

- Docker installed and running
- Access to `docker.turing-trading.org` registry
  - Registry credentials are in `deploy/dockerconfig.json`
  - Build script will automatically login
- kubectl configured with access to your Kubernetes cluster
- Credentials for the flashbots PostgreSQL database

## Quick Start

### 1. Update Secrets

Edit `k8s/secret.yaml` and replace the placeholder values:

```yaml
POSTGRE_PASSWORD: "YOUR_ACTUAL_PASSWORD"
ETH_WS_URL: "wss://YOUR_WEBSOCKET_ENDPOINT"
```

**Important**: Never commit real secrets to git! Consider using:
- Kubernetes External Secrets Operator
- Sealed Secrets
- HashiCorp Vault
- Manual secret creation with `kubectl create secret`

### 2. Build and Push Docker Image

```bash
# Build and push with default tag (latest)
./deploy/build.sh

# Or with a specific version tag
./deploy/build.sh v1.0.0
```

### 3. Deploy to Kubernetes

```bash
# Create namespace and deploy all resources
kubectl apply -f deploy/k8s/

# Verify deployment
kubectl get pods -n flashbots
kubectl logs -f deployment/flashbots-live -n flashbots
```

## Manual Docker Build

If you prefer to build manually:

```bash
# Login to registry
docker login docker.turing-trading.org -u admin -p "9yAsA1k8n#B@IfzE6N8f"

# Build
docker build -f deploy/Dockerfile -t docker.turing-trading.org/flashbots/mev-boost:latest .

# Push
docker push docker.turing-trading.org/flashbots/mev-boost:latest
```

## Kubernetes Resources

### Namespace (`k8s/namespace.yaml`)
Creates the `flashbots` namespace for isolation.

### Docker Registry Secret (`k8s/docker-registry-secret.yaml`)
Contains credentials for pulling images from `docker.turing-trading.org`:
- Username: admin
- Password: (base64 encoded)
- Auth token: (base64 encoded)

**Note**: This secret is generated from `deploy/dockerconfig.json`

### Secret (`k8s/secret.yaml`)
Contains:
- PostgreSQL connection details
- Ethereum RPC/WebSocket endpoints

**Security Note**: Replace placeholder values before deploying!

### Deployment (`k8s/deployment.yaml`)
Deploys the live streaming application with:
- **1 replica** (avoid duplicate processing)
- **Recreate strategy** (ensure only one instance runs)
- Resource limits: 2Gi RAM, 2 CPU cores
- Resource requests: 512Mi RAM, 0.5 CPU cores
- Liveness/readiness probes
- Non-root security context

## Monitoring

Check application logs:

```bash
# Follow logs
kubectl logs -f deployment/flashbots-live -n flashbots

# Get last 100 lines
kubectl logs deployment/flashbots-live -n flashbots --tail=100

# View logs from specific pod
kubectl logs -f <pod-name> -n flashbots
```

Check pod status:

```bash
kubectl get pods -n flashbots
kubectl describe pod <pod-name> -n flashbots
```

## Updating the Deployment

### Rolling Update

```bash
# Build and push new image
./deploy/build.sh v1.0.1

# Update deployment image
kubectl set image deployment/flashbots-live \
    flashbots-live=docker.turing-trading.org/flashbots/mev-boost:v1.0.1 \
    -n flashbots

# Or restart to pull latest
kubectl rollout restart deployment/flashbots-live -n flashbots
```

### Force Recreate

```bash
kubectl delete pod -l app=flashbots-live -n flashbots
```

## Troubleshooting

### Pod not starting

```bash
# Check events
kubectl describe pod <pod-name> -n flashbots

# Check logs
kubectl logs <pod-name> -n flashbots
```

### Database connection issues

```bash
# Verify secrets
kubectl get secret flashbots-live-secrets -n flashbots -o yaml

# Test database connectivity from pod
kubectl exec -it <pod-name> -n flashbots -- python -c "
from src.helpers.db import AsyncSessionLocal
import asyncio

async def test():
    async with AsyncSessionLocal() as session:
        print('Database connection successful!')

asyncio.run(test())
"
```

### WebSocket connection issues

Check logs for connection errors:

```bash
kubectl logs deployment/flashbots-live -n flashbots | grep -i "websocket\|connect"
```

## Scaling

**Important**: This application should only run 1 replica to avoid duplicate block processing.

If you need high availability:
1. Use leader election (requires code changes)
2. Implement distributed locking
3. Use a message queue for deduplication

## Cleanup

Remove all resources:

```bash
kubectl delete -f deploy/k8s/
```

## Production Considerations

1. **Secrets Management**: Use a proper secrets management solution
2. **Monitoring**: Add Prometheus metrics and alerts
3. **Logging**: Consider centralized logging (ELK, Loki)
4. **Backups**: Ensure database backups are configured
5. **Auto-restart**: Deployment has `restartPolicy: Always`
6. **Resource Tuning**: Adjust resource limits based on actual usage
7. **Network Policies**: Add network policies for security
8. **RBAC**: Configure service accounts and RBAC policies

## Architecture

```
┌─────────────────────────────────────┐
│   Kubernetes Cluster (flashbots)   │
│                                     │
│  ┌───────────────────────────────┐ │
│  │  Deployment: flashbots-live   │ │
│  │  Replicas: 1                  │ │
│  │                               │ │
│  │  ┌─────────────────────────┐ │ │
│  │  │  Container              │ │ │
│  │  │  - WebSocket stream     │ │ │
│  │  │  - Block processor      │ │ │
│  │  │  - Relay processor      │ │ │
│  │  │  - Proposer processor   │ │ │
│  │  │  - Builder processor    │ │ │
│  │  │  - Analysis processor   │ │ │
│  │  └─────────────────────────┘ │ │
│  └───────────────────────────────┘ │
│                                     │
│  ┌───────────────────────────────┐ │
│  │  Secret: flashbots-live-secrets││
│  │  - DB credentials            │ │
│  │  - Ethereum endpoints        │ │
│  └───────────────────────────────┘ │
└─────────────────────────────────────┘
            │
            ├─────> TimescaleDB (PostgreSQL)
            │
            └─────> Ethereum WebSocket/RPC
```
