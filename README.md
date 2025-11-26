# Strimzi Stretch Cluster - LoadBalancer Networking Provider

This plugin provides LoadBalancer-based networking for Strimzi Kafka stretch clusters deployed across multiple Kubernetes clusters.

## Overview

The LoadBalancer provider creates LoadBalancer services for each Kafka broker pod to enable cross-cluster communication. It's suitable for cloud environments where LoadBalancer services are readily available (AWS, GCP, Azure, on-premises with MetalLB, etc.).

## Features

- **Per-Pod LoadBalancer Services**: Creates a dedicated LoadBalancer service for each Kafka broker
- **Automatic IP Discovery**: Waits for and discovers LoadBalancer external IP/hostname automatically
- **Cloud Provider Agnostic**: Works with any Kubernetes LoadBalancer implementation
- **Resilient**: Uses exponential backoff retry logic (up to 5 minutes) for IP assignment

## Prerequisites

- Kubernetes cluster with LoadBalancer support:
  - Cloud providers: AWS ELB/NLB, GCP Load Balancer, Azure Load Balancer
  - On-premises: MetalLB, Cilium BGP, or similar
- Network connectivity between all clusters
- Strimzi Operator 0.48.0 or later

## How It Works

1. **Service Creation**: Creates a LoadBalancer service for each broker pod with selector targeting the specific pod
2. **IP Assignment Wait**: Waits for cloud provider to assign external IP/hostname (uses exponential backoff, max 5 minutes)
3. **Endpoint Discovery**: Extracts the assigned IP/hostname and port from service status
4. **Configuration**: Generates `advertised.listeners` and `controller.quorum.voters` using LoadBalancer endpoints

### Example Resources Created

For a broker pod `my-cluster-kafka-0`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-cluster-kafka-0-lb
  namespace: kafka
spec:
  type: LoadBalancer
  selector:
    statefulset.kubernetes.io/pod-name: my-cluster-kafka-0
  externalTrafficPolicy: Local
  ports:
  - name: tcp-replication
    port: 9091
    protocol: TCP
  - name: tcp-plain
    port: 9092
    protocol: TCP
```

## Installation

### 1. Build the Plugin

```bash
cd strimzi-stretch-loadbalancer-plugin
mvn clean package
```

This produces `target/strimzi-stretch-loadbalancer-plugin-0.48.0.jar`

### 2. Deploy Plugin to Operator

Add the plugin JAR to the Strimzi operator image or mount it as a volume:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: strimzi-cluster-operator
spec:
  template:
    spec:
      containers:
      - name: strimzi-cluster-operator
        volumeMounts:
        - name: stretch-plugins
          mountPath: /opt/strimzi/plugins/stretch
      volumes:
      - name: stretch-plugins
        configMap:
          name: stretch-plugins
```

### 3. Configure Operator

Set environment variables to enable the plugin:

```yaml
env:
- name: STRIMZI_STRETCH_PLUGIN_CLASS_NAME
  value: io.strimzi.plugin.stretch.LoadBalancerNetworkingProvider
- name: STRIMZI_STRETCH_PLUGIN_CLASS_PATH
  value: /opt/strimzi/plugins/stretch/*
```

### 4. Configure Kafka CR

Enable stretch mode in your Kafka custom resource:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    config:
      # ... other config
    # Stretch cluster configuration
    stretch:
      enabled: true
      clusters:
        - id: cluster-1
          # ... cluster config
        - id: cluster-2
          # ... cluster config
```

## Configuration Options

Currently, the LoadBalancer provider doesn't require additional configuration. Future versions may support:

- LoadBalancer annotations (for cloud-specific settings)
- Service class specification
- Health check configuration
- Timeout customization

## Network Requirements

### Firewall Rules

Ensure the following ports are accessible from all clusters:

- **9091** - Kafka replication (inter-broker communication)
- **9090** - KRaft controller communication
- **User-defined listener ports** - Client access (e.g., 9092 for PLAIN)

### LoadBalancer IP Ranges

For cloud providers:
- Configure appropriate IP address pools
- Ensure DNS resolution if using hostnames

For MetalLB (on-premises):
```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kafka-pool
  namespace: metallb-system
spec:
  addresses:
  - 10.21.50.10-10.21.50.50  # Adjust to your network
```

## Troubleshooting

### LoadBalancer IP Not Assigned

**Symptom**: Operator logs show timeout waiting for LoadBalancer IP

```
Timeout waiting for LoadBalancer IP assignment on service my-cluster-kafka-0-lb 
after 30 attempts (~5 minutes)
```

**Solutions**:
- Check cloud provider quota/limits for LoadBalancers
- Verify LoadBalancer controller is running (MetalLB, cloud controller manager)
- Check service events: `kubectl describe svc my-cluster-kafka-0-lb`
- For MetalLB: Verify IPAddressPool configuration

### Cross-Cluster Communication Fails

**Symptom**: Kafka brokers can't communicate across clusters

**Solutions**:
- Verify network connectivity: `ping <loadbalancer-ip>` from other clusters
- Check firewall rules allow traffic on ports 9090, 9091, and listener ports
- Verify LoadBalancer service status: `kubectl get svc -o wide`
- Check Kafka logs for connection errors

### Slow Reconciliation

**Symptom**: Operator takes long time to reconcile

**Cause**: LoadBalancer IP assignment can take 2-5 minutes per broker

**Expected Behavior**: This is normal. The provider uses exponential backoff and waits up to 5 minutes per broker for IP assignment.

## Performance Characteristics

- **Service Creation**: ~1 second per broker
- **IP Assignment Wait**: 10 seconds to 5 minutes (cloud provider dependent)
- **API Calls per Reconciliation**: ~2 per broker (service reconcile + get)
- **Recommended Maximum**: 100 brokers per cluster (LoadBalancer quota dependent)

## Comparison with Other Providers

| Feature | LoadBalancer | NodePort | MCS |
|---------|-------------|----------|-----|
| Service Type | LoadBalancer | NodePort | ClusterIP + ServiceExport |
| External IP | Cloud-provided | Node IP | DNS-based |
| Cloud Native | ✅ Yes | ⚠️ Partial | ❌ No (multi-cluster only) |
| Cost | 💰💰 High | 💰 Low | 💰 Low |
| Performance | ⚡⚡⚡ Excellent | ⚡⚡ Good | ⚡⚡ Good |
| Setup Complexity | Easy | Easy | Complex |
| Best For | Production cloud | Development/On-prem | Multi-cluster mesh |

## Example Deployment

See the complete guide at `/stretch_lb_complete_guide.md` in the parent repository.

## Building and Testing

```bash
# Build
mvn clean package

# Run tests (when available)
mvn test

# Integration test (requires Kubernetes clusters)
# See /stretch_lb_complete_guide.md
```

## Contributing

This plugin follows Strimzi contribution guidelines. See the main Strimzi repository for details.

## License

Apache License 2.0 - See LICENSE file in the root directory.

## Support

For issues and questions:
- Strimzi Slack: #strimzi channel
- GitHub Issues: [strimzi/strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator)
- Mailing List: [Strimzi Dev List](https://lists.cncf.io/g/cncf-strimzi-dev)

## Version

- Plugin Version: 0.48.0
- Compatible with: Strimzi 0.48.0+
- Kubernetes Version: 1.25+
