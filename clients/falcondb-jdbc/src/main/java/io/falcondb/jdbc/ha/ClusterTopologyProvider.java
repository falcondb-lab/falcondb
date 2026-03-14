package io.falcondb.jdbc.ha;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Provides cluster topology information for HA-aware failover.
 * Maintains a list of known nodes and tracks the current primary.
 */
public class ClusterTopologyProvider {

    private final List<NodeInfo> nodes = new CopyOnWriteArrayList<>();
    private volatile NodeInfo primary;
    private volatile long lastRefreshMs;
    private final long refreshIntervalMs;

    public ClusterTopologyProvider(List<NodeInfo> seedNodes, long refreshIntervalMs) {
        this.refreshIntervalMs = refreshIntervalMs;
        this.nodes.addAll(seedNodes);
        if (!seedNodes.isEmpty()) {
            this.primary = seedNodes.get(0);
        }
        this.lastRefreshMs = System.currentTimeMillis();
    }

    public ClusterTopologyProvider(String host, int port) {
        this(Collections.singletonList(new NodeInfo(host, port, 1, true)), 30_000);
    }

    /**
     * Get the current primary node. Returns null if unknown.
     */
    public NodeInfo getPrimary() {
        return primary;
    }

    /**
     * Get all known nodes.
     */
    public List<NodeInfo> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    /**
     * Update topology after receiving server info (e.g. from ServerHello or error response).
     */
    public void updatePrimary(String host, int port, long epoch) {
        NodeInfo newPrimary = null;
        for (NodeInfo n : nodes) {
            if (n.host.equals(host) && n.port == port) {
                newPrimary = n;
                break;
            }
        }
        if (newPrimary == null) {
            newPrimary = new NodeInfo(host, port, epoch, true);
            nodes.add(newPrimary);
        }
        // Demote old primary
        if (primary != null && primary != newPrimary) {
            primary = new NodeInfo(primary.host, primary.port, primary.epoch, false);
        }
        this.primary = newPrimary;
        this.lastRefreshMs = System.currentTimeMillis();
    }

    /**
     * Mark a node as failed (e.g. after connection error).
     */
    public void markFailed(String host, int port) {
        if (primary != null && primary.host.equals(host) && primary.port == port) {
            primary = null;
        }
    }

    /**
     * Whether the topology cache is stale and should be refreshed.
     */
    public boolean isStale() {
        return System.currentTimeMillis() - lastRefreshMs > refreshIntervalMs;
    }

    /**
     * Get a candidate node for failover (any node that isn't the failed one).
     */
    public NodeInfo getFailoverCandidate(String failedHost, int failedPort) {
        for (NodeInfo n : nodes) {
            if (!(n.host.equals(failedHost) && n.port == failedPort)) {
                return n;
            }
        }
        return null;
    }

    /**
     * Cluster node information.
     */
    public static class NodeInfo {
        public final String host;
        public final int port;
        public final long epoch;
        public final boolean isPrimary;

        public NodeInfo(String host, int port, long epoch, boolean isPrimary) {
            this.host = host;
            this.port = port;
            this.epoch = epoch;
            this.isPrimary = isPrimary;
        }

        @Override
        public String toString() {
            return host + ":" + port + (isPrimary ? " (primary)" : " (replica)") + " epoch=" + epoch;
        }
    }
}
