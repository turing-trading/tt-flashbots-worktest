# Quick Start - Deploy Flashbots Live Streaming

## 1. Prerequisites

Ensure Docker daemon is running:
```bash
docker ps
```

## 2. Update Secrets

Edit `deploy/k8s/secret.yaml`:
```yaml
POSTGRE_PASSWORD: "<your-postgres-password>"
ETH_WS_URL: "wss://<your-ethereum-websocket-endpoint>"
```

## 3. Build & Push Image

```bash
./deploy/build.sh
```

Expected output:
```
Building Docker image: docker.turing-trading.org/flashbots/mev-boost:latest
...
✅ Image pushed successfully!

Image: docker.turing-trading.org/flashbots/mev-boost:latest
```

## 4. Deploy to Kubernetes

```bash
# Deploy all resources
kubectl apply -f deploy/k8s/

# Verify deployment
kubectl get pods -n flashbots
```

Expected output:
```
NAME                              READY   STATUS    RESTARTS   AGE
flashbots-live-xxxxxxxxxx-xxxxx   1/1     Running   0          30s
```

## 5. Monitor Logs

```bash
kubectl logs -f deployment/flashbots-live -n flashbots
```

Expected output:
```
2025-11-18 00:57:24 - INFO - Connecting to wss://...
2025-11-18 00:57:24 - INFO - Live block processor started
2025-11-18 00:57:24 - INFO - Successfully subscribed to newHeads
2025-11-18 00:57:27 - INFO - New block #23822289 hash=0x0fa4e901...
2025-11-18 00:57:27 - INFO - Stored block #23822289
...
```

## Troubleshooting

### Docker not running
```bash
# Start Docker/Colima
colima start
```

### Build fails
```bash
# Check Dockerfile syntax
docker build -f deploy/Dockerfile -t test .
```

### Pod not starting
```bash
# Check pod status
kubectl describe pod -n flashbots -l app=flashbots-live

# Check logs
kubectl logs -n flashbots -l app=flashbots-live --tail=50
```

### Update secrets after deployment
```bash
# Edit secret
kubectl edit secret flashbots-live-secrets -n flashbots

# Restart deployment to pick up new secrets
kubectl rollout restart deployment/flashbots-live -n flashbots
```

## Common Commands

```bash
# View logs
kubectl logs -f deployment/flashbots-live -n flashbots

# Restart deployment
kubectl rollout restart deployment/flashbots-live -n flashbots

# Scale down (stop)
kubectl scale deployment/flashbots-live --replicas=0 -n flashbots

# Scale up (start)
kubectl scale deployment/flashbots-live --replicas=1 -n flashbots

# Delete deployment
kubectl delete deployment flashbots-live -n flashbots

# Delete everything
kubectl delete namespace flashbots
```
