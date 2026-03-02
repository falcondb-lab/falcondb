# FalconDB Helm Chart

Deploy FalconDB on Kubernetes with production-grade defaults.

## Quick Start

```bash
# Standalone (single node)
helm install falcondb ./deploy/helm/falcondb \
  --set falcondb.auth.password=changeme

# Cluster (primary + replica)
helm install falcondb ./deploy/helm/falcondb \
  --set cluster.enabled=true \
  --set cluster.replicas=2 \
  --set falcondb.auth.password=changeme
```

## Connect

```bash
# Port-forward
kubectl port-forward svc/falcondb 5443:5443

# psql
psql -h 127.0.0.1 -p 5443 -U falcon
```

## Configuration

See `values.yaml` for all options. Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `falcondb.role` | `standalone` | Node role: standalone, primary, replica |
| `falcondb.auth.method` | `scram-sha-256` | Auth method |
| `falcondb.auth.password` | `""` | DB password (required) |
| `falcondb.tls.enabled` | `false` | Enable TLS |
| `persistence.enabled` | `true` | Persistent storage |
| `persistence.size` | `10Gi` | Volume size |
| `serviceMonitor.enabled` | `false` | Prometheus ServiceMonitor |
| `cluster.enabled` | `false` | Deploy as StatefulSet cluster |

## TLS

```bash
helm install falcondb ./deploy/helm/falcondb \
  --set falcondb.tls.enabled=true \
  --set falcondb.tls.existingSecret=falcondb-tls
```

## Monitoring

Enable Prometheus scraping (annotations-based):
```bash
helm install falcondb ./deploy/helm/falcondb \
  --set metrics.enabled=true
```

Or with Prometheus Operator ServiceMonitor:
```bash
helm install falcondb ./deploy/helm/falcondb \
  --set serviceMonitor.enabled=true
```

## Persistence

Data is stored on a PVC mounted at `/var/lib/falcondb`. To use a specific storage class:

```bash
helm install falcondb ./deploy/helm/falcondb \
  --set persistence.storageClass=fast-ssd
```

## Uninstall

```bash
helm uninstall falcondb
# PVCs are NOT deleted automatically — remove manually if needed:
kubectl delete pvc -l app.kubernetes.io/instance=falcondb
```
