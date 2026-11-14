package io.quorumdb.server.transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Live network faults for this node, applied by {@link GrpcTransport} to outgoing <em>and</em>
 * incoming messages so that a fault configured on one side is still symmetric. Set by the live
 * FaultLab controller; everything here is thread-safe.
 */
public final class NetworkFaults {
    private final Set<String> blocked = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> delayMs = new ConcurrentHashMap<>();
    private final Map<String, Double> dropProbability = new ConcurrentHashMap<>();
    private volatile long allDelayMs;
    private volatile double allDropProbability;

    public void block(Set<String> peers) {
        blocked.addAll(peers);
    }

    public void unblock(Set<String> peers) {
        blocked.removeAll(peers);
    }

    public void setDelay(long ms) {
        allDelayMs = Math.max(0, ms);
    }

    public void setDrop(double probability) {
        allDropProbability = Math.max(0, Math.min(1, probability));
    }

    public void setPeerDelay(String peer, long ms) {
        if (ms <= 0) delayMs.remove(peer); else delayMs.put(peer, ms);
    }

    public void setPeerDrop(String peer, double p) {
        if (p <= 0) dropProbability.remove(peer); else dropProbability.put(peer, Math.min(1, p));
    }

    public void healAll() {
        blocked.clear();
        delayMs.clear();
        dropProbability.clear();
        allDelayMs = 0;
        allDropProbability = 0;
    }

    public boolean isBlocked(String peer) {
        return blocked.contains(peer);
    }

    /** Decides the fate of one message to or from {@code peer}. */
    public boolean shouldDrop(String peer) {
        if (blocked.contains(peer)) return true;
        double p = Math.max(allDropProbability, dropProbability.getOrDefault(peer, 0.0));
        return p > 0 && ThreadLocalRandom.current().nextDouble() < p;
    }

    public long delayFor(String peer) {
        return Math.max(allDelayMs, delayMs.getOrDefault(peer, 0L));
    }

    public Set<String> blockedPeers() {
        return Set.copyOf(blocked);
    }

    public long globalDelayMs() {
        return allDelayMs;
    }

    public double globalDropProbability() {
        return allDropProbability;
    }

    public boolean active() {
        return !blocked.isEmpty() || !delayMs.isEmpty() || !dropProbability.isEmpty() || allDelayMs > 0
                || allDropProbability > 0;
    }
}
