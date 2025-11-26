# Strimzi Stretch Cluster - LoadBalancer Networking Provider

LoadBalancer-based networking provider plugin for Strimzi Kafka stretch clusters deployed across multiple Kubernetes clusters.

## What This Plugin Does

This plugin enables cross-cluster Kafka communication by:

1. **Creating LoadBalancer Services** - Creates a dedicated LoadBalancer service for each Kafka broker pod
2. **Waiting for IP Assignment** - Waits for cloud provider to assign external IP/hostname (with exponential backoff retry, up to 5 minutes)
3. **Discovering Endpoints** - Extracts the assigned LoadBalancer IP/hostname and port
4. **Generating Configurations** - Automatically generates `advertised.listeners` and `controller.quorum.voters` configurations for Kafka brokers

**Example Output:**
- Service: `my-cluster-kafka-0-lb` with external IP `10.21.50.10`
- Advertised Listener: `REPLICATION-9091://10.21.50.10:9091`
- Controller Quorum Voter: `0@10.21.50.10:9091`

## How It Works

### 1. Service Creation
For each broker pod, the plugin creates a LoadBalancer service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-cluster-kafka-0-lb
spec:
  type: LoadBalancer
  selector:
    statefulset.kubernetes.io/pod-name: my-cluster-kafka-0
  ports:
  - name: tcp-replication
    port: 9091
  - name: tcp-plain
    port: 9092
```

### 2. IP Assignment Wait
The plugin waits for the cloud provider/LoadBalancer controller to assign an external IP:
- **Retry Logic**: Exponential backoff (10s → 20s → 30s...)
- **Maximum Wait**: 5 minutes per broker
- **Cloud Provider Dependent**: AWS ELB/NLB, GCP LB, Azure LB, MetalLB, etc.

### 3. Endpoint Discovery
Once the IP is assigned, the plugin extracts it from `service.status.loadBalancer.ingress[0]`:
- IP address: `10.21.50.10`
- Or hostname: `a1b2c3-123.us-west-2.elb.amazonaws.com`

## Prerequisites

**LoadBalancer Support Required:**
- **Cloud**: AWS ELB/NLB, GCP Load Balancer, Azure Load Balancer
- **On-Premises**: MetalLB, Cilium BGP, or similar LoadBalancer controller
- **Network**: Connectivity between all cluster LoadBalancer IPs

## Building the Plugin

```bash
cd strimzi-stretch-loadbalancer-plugin
mvn clean package
```

This produces: `target/strimzi-stretch-loadbalancer-plugin-0.48.0.jar`

## Deploying the Plugin

### 1. Add JAR to Operator

Mount the plugin JAR into the Strimzi operator:

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
          mountPath: /opt/strimzi/plugins
      volumes:
      - name: stretch-plugins
        hostPath:
          path: /opt/strimzi/plugins  # Copy JAR here on operator node
```

### 2. Configure Operator Environment Variables

Enable the plugin by setting these environment variables in the operator deployment:

```yaml
env:
- name: STRIMZI_STRETCH_PLUGIN_CLASS_NAME
  value: io.strimzi.plugin.stretch.LoadBalancerNetworkingProvider
- name: STRIMZI_STRETCH_PLUGIN_CLASS_PATH
  value: /opt/strimzi/plugins/*
```

## Network Requirements

**LoadBalancer Controller:**
- Ensure LoadBalancer controller is running (cloud provider controller or MetalLB)
- Configure appropriate IP address pools

**For MetalLB (on-premises example):**
```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kafka-pool
  namespace: metallb-system
spec:
  addresses:
  - 10.21.50.10-10.21.50.50
```

**Firewall:**
- Allow traffic on Kafka ports (9090, 9091, 9092, etc.)
- LoadBalancer IPs must be routable between clusters

## Troubleshooting

### LoadBalancer IP Not Assigned

**Symptom:** Timeout after ~5 minutes waiting for LoadBalancer IP

**Solutions:**
- Check cloud provider LoadBalancer quota/limits
- Verify LoadBalancer controller is running: `kubectl get pods -n metallb-system` (for MetalLB)
- Check service events: `kubectl describe svc my-cluster-kafka-0-lb`
- Verify IPAddressPool configuration (for MetalLB)

### Slow Reconciliation

**This is normal.** LoadBalancer IP assignment can take 2-5 minutes per broker depending on the cloud provider. The plugin uses exponential backoff and waits up to 5 minutes per broker.

## License

Apache License 2.0

## Version

- Plugin Version: 0.48.0
- Compatible with: Strimzi 0.48.0+
- Requires: Kubernetes 1.25+ with LoadBalancer support
