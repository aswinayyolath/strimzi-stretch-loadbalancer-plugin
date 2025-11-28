/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * LoadBalancer-based networking provider for stretch clusters.
 * 
 * Creates LoadBalancer services for cross-cluster communication.
 * Uses LoadBalancer IPs or Hostnames for addressing.
 */
public class LoadBalancerNetworkingProvider implements StretchNetworkingProvider {
    private static final Logger LOGGER = LogManager.getLogger(LoadBalancerNetworkingProvider.class);

    private ResourceOperatorSupplier centralSupplier;
    private RemoteResourceOperatorSupplier remoteResourceOperatorSupplier;
    
    @Override
    public Future<Void> init(Map<String, String> config, 
                             ResourceOperatorSupplier centralSupplier, 
                             RemoteResourceOperatorSupplier remoteResourceOperatorSupplier) {
        this.centralSupplier = centralSupplier;
        this.remoteResourceOperatorSupplier = remoteResourceOperatorSupplier;
        LOGGER.info("Initialized LoadBalancer networking provider");
        return Future.succeededFuture();
    }

    @Override
    public Future<List<HasMetadata>> createNetworkingResources(Reconciliation reconciliation, 
                                                               String namespace, 
                                                               String podName, 
                                                               String clusterId, 
                                                               Map<String, Integer> ports) {
        
        LOGGER.debug("{}: Creating LoadBalancer resources for pod {} in cluster {}", 
                reconciliation, podName, clusterId);

        // Get the supplier for the target cluster
        ResourceOperatorSupplier supplier = getSupplier(clusterId);
        if (supplier == null) {
            return Future.failedFuture("No supplier found for cluster " + clusterId);
        }

        // Create per-pod LoadBalancer service
        String serviceName = podName + "-lb";
        
        List<ServicePort> servicePorts = ports.entrySet().stream()
            .map(entry -> new ServicePortBuilder()
                .withName(entry.getKey())
                .withPort(entry.getValue())
                .withTargetPort(new io.fabric8.kubernetes.api.model.IntOrString(entry.getValue()))
                .withProtocol("TCP")
                .build())
            .collect(Collectors.toList());

        // Selector matches the specific pod
        Map<String, String> selector = new HashMap<>();
        selector.put("statefulset.kubernetes.io/pod-name", podName);

        Service service = new ServiceBuilder()
            .withNewMetadata()
                .withName(serviceName)
                .withNamespace(namespace)
                .addToLabels("app", "strimzi")
                .addToLabels("strimzi.io/cluster", reconciliation.name())
                .addToLabels("strimzi.io/kind", "Kafka")
                .addToLabels("strimzi.io/name", reconciliation.name() + "-kafka")
                .addToAnnotations("strimzi.io/stretch-cluster-id", clusterId)
            .endMetadata()
            .withNewSpec()
                .withType("LoadBalancer")
                .withPorts(servicePorts)
                .withSelector(selector)
                .withExternalTrafficPolicy("Local")
            .endSpec()
            .build();

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, service)
            .map(s -> Collections.singletonList((HasMetadata) s));
    }

    @Override
    public Future<String> discoverPodEndpoint(Reconciliation reconciliation, 
                                              String namespace, 
                                              String podName, 
                                              String clusterId, 
                                              String portName) {
        
        String serviceName = podName + "-lb";
        ResourceOperatorSupplier supplier = getSupplier(clusterId);
        
        if (supplier == null) {
            return Future.failedFuture("No supplier found for cluster " + clusterId);
        }

        // Wait for LoadBalancer to be ready, then extract endpoint
        return waitForLoadBalancerReady(reconciliation, supplier, namespace, serviceName, 0)
            .compose(service -> {
                LoadBalancerIngress ingress = service.getStatus().getLoadBalancer().getIngress().get(0);
                String host = ingress.getHostname() != null ? ingress.getHostname() : ingress.getIp();
                
                if (host == null) {
                    return Future.failedFuture("LoadBalancer status has no IP or Hostname for service " + serviceName);
                }

                // Find the port
                Integer port = service.getSpec().getPorts().stream()
                    .filter(p -> p.getName().equals(portName))
                    .map(ServicePort::getPort)
                    .findFirst()
                    .orElse(null);

                if (port == null) {
                    return Future.failedFuture("Port " + portName + " not found in service " + serviceName);
                }

                LOGGER.debug("{}: Discovered LoadBalancer endpoint for pod {} in cluster {}: {}:{}", 
                    reconciliation, podName, clusterId, host, port);
                return Future.succeededFuture(host + ":" + port);
            });
    }

    /**
     * Wait for LoadBalancer to get an external IP/hostname assigned.
     * Uses exponential backoff with max wait time of 5 minutes.
     * 
     * @param reconciliation Reconciliation context
     * @param supplier Resource operator supplier for the target cluster
     * @param namespace Kubernetes namespace
     * @param serviceName LoadBalancer service name
     * @param attempt Current attempt number (starts at 0)
     * @return Future with the ready Service object
     */
    private Future<Service> waitForLoadBalancerReady(Reconciliation reconciliation,
                                                      ResourceOperatorSupplier supplier,
                                                      String namespace,
                                                      String serviceName,
                                                      int attempt) {
        return supplier.serviceOperations.getAsync(namespace, serviceName)
            .compose(service -> {
                if (service == null) {
                    return Future.failedFuture("Service " + serviceName + " not found");
                }

                // Check if LoadBalancer is ready
                if (service.getStatus() != null && 
                    service.getStatus().getLoadBalancer() != null &&
                    service.getStatus().getLoadBalancer().getIngress() != null &&
                    !service.getStatus().getLoadBalancer().getIngress().isEmpty()) {
                    
                    LoadBalancerIngress ingress = service.getStatus().getLoadBalancer().getIngress().get(0);
                    if (ingress.getIp() != null || ingress.getHostname() != null) {
                        LOGGER.info("{}: LoadBalancer {} is ready with IP/hostname", 
                            reconciliation, serviceName);
                        return Future.succeededFuture(service);
                    }
                }

                // Max attempts: 30 attempts with exponential backoff = ~5 minutes total
                if (attempt >= 30) {
                    return Future.failedFuture(
                        "Timeout waiting for LoadBalancer IP assignment on service " + serviceName + 
                        " after " + attempt + " attempts (~5 minutes). Check cloud provider LoadBalancer provisioning.");
                }

                // Exponential backoff: 5s, 10s, 20s, 40s, then capped at 60s
                long delayMs = Math.min(5000L * (1L << attempt), 60000L);
                
                LOGGER.debug("{}: LoadBalancer {} not ready yet, will retry in {}ms (attempt {}/30)", 
                    reconciliation, serviceName, delayMs, attempt + 1);

                // Schedule retry after delay using Vertx from current context
                io.vertx.core.Promise<Service> promise = io.vertx.core.Promise.promise();
                io.vertx.core.Vertx.currentContext().owner().setTimer(delayMs, timer -> {
                    waitForLoadBalancerReady(reconciliation, supplier, namespace, serviceName, attempt + 1)
                        .onComplete(promise);
                });
                return promise.future();
            });
    }

    @Override
    public String generateServiceDnsName(String namespace, String serviceName, String clusterId) {
        // LoadBalancer might use DNS if provided by the cloud provider,
        // but typically we rely on the IP/Hostname from status.
        // We can return the local service name as fallback.
        return serviceName + "." + namespace + ".svc";
    }

    @Override
    public String generatePodDnsName(String namespace, String serviceName, String podName, String clusterId) {
        // Similar fallback
        return podName + "." + serviceName + "." + namespace + ".svc";
    }

    @Override
    public Future<String> generateAdvertisedListeners(Reconciliation reconciliation, 
                                                      String namespace, 
                                                      String podName, 
                                                      String clusterId, 
                                                      Map<String, String> listeners) {
        
        List<Future<String>> futures = new ArrayList<>();
        List<String> listenerKeys = new ArrayList<>();

        for (Map.Entry<String, String> entry : listeners.entrySet()) {
            String listenerName = entry.getKey();
            String portName = entry.getValue();
            listenerKeys.add(listenerName);
            
            futures.add(discoverPodEndpoint(reconciliation, namespace, podName, clusterId, portName));
        }

        return Future.join(futures)
            .map(result -> {
                List<String> advertisedListeners = new ArrayList<>();
                for (int i = 0; i < result.size(); i++) {
                    String endpoint = result.resultAt(i);
                    String listenerName = listenerKeys.get(i);
                    advertisedListeners.add(listenerName + "://" + endpoint);
                }
                return String.join(",", advertisedListeners);
            });
    }

    @Override
    public Future<String> generateQuorumVoters(Reconciliation reconciliation, 
                                               String namespace, 
                                               List<ControllerPodInfo> controllerPods, 
                                               String replicationPortName) {
        
        List<Future<String>> futures = new ArrayList<>();
        List<Integer> nodeIds = new ArrayList<>();

        for (ControllerPodInfo info : controllerPods) {
            nodeIds.add(info.nodeId());
            futures.add(discoverPodEndpoint(reconciliation, namespace, info.podName(), info.clusterId(), replicationPortName));
        }

        return Future.join(futures)
            .map(result -> {
                List<String> voters = new ArrayList<>();
                for (int i = 0; i < result.size(); i++) {
                    String endpoint = result.resultAt(i);
                    int nodeId = nodeIds.get(i);
                    voters.add(nodeId + "@" + endpoint);
                }
                return String.join(",", voters);
            });
    }

    @Override
    public Future<Void> deleteNetworkingResources(Reconciliation reconciliation, 
                                                  String namespace, 
                                                  String podName, 
                                                  String clusterId) {
        String serviceName = podName + "-lb";
        ResourceOperatorSupplier supplier = getSupplier(clusterId);
        
        if (supplier == null) {
            return Future.succeededFuture();
        }

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, null)
            .mapEmpty();
    }

    @Override
    public String getProviderName() {
        return "load-balancer";
    }

    private ResourceOperatorSupplier getSupplier(String clusterId) {
        if (remoteResourceOperatorSupplier.remoteResourceOperators.containsKey(clusterId)) {
            return remoteResourceOperatorSupplier.get(clusterId);
        }
        return centralSupplier;
    }
}
