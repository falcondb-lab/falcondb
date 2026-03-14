package io.falcondb.jdbc.ha;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Caches the current primary endpoint with a TTL.
 * Thread-safe for concurrent driver usage.
 */
public class PrimaryResolver {

    private final ClusterTopologyProvider topology;
    private final AtomicReference<ClusterTopologyProvider.NodeInfo> cached = new AtomicReference<>();
    private final AtomicLong cachedAtMs = new AtomicLong(0);
    private final long ttlMs;

    public PrimaryResolver(ClusterTopologyProvider topology, long ttlMs) {
        this.topology = topology;
        this.ttlMs = ttlMs;
    }

    public PrimaryResolver(ClusterTopologyProvider topology) {
        this(topology, 10_000); // 10s default TTL
    }

    /**
     * Resolve the current primary. Returns cached value if within TTL.
     */
    public ClusterTopologyProvider.NodeInfo resolve() {
        ClusterTopologyProvider.NodeInfo node = cached.get();
        long age = System.currentTimeMillis() - cachedAtMs.get();
        if (node != null && age < ttlMs) {
            return node;
        }
        // Refresh from topology
        node = topology.getPrimary();
        if (node != null) {
            cached.set(node);
            cachedAtMs.set(System.currentTimeMillis());
        }
        return node;
    }

    /**
     * Invalidate the cache, forcing a refresh on next resolve().
     */
    public void invalidate() {
        cached.set(null);
        cachedAtMs.set(0);
    }

    /**
     * Update the cached primary after a failover event.
     */
    public void updatePrimary(String host, int port, long epoch) {
        topology.updatePrimary(host, port, epoch);
        invalidate();
    }
}
