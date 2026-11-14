package io.quorumdb.server.api;

import io.quorumdb.server.transport.GrpcTransport;

import java.util.List;
import java.util.Map;

/**
 * Everything the dashboard shows about one node. Produced by the node itself ({@code GET /api/v1/node})
 * and collected by any node for the cluster-wide views. {@code reachable=false} means the node did not
 * answer; then only the identity fields are filled.
 *
 * @param health UP (serving, leader known), DEGRADED (serving, no leader known), DOWN (crashed or failed),
 *               UNREACHABLE (no answer)
 */
public record NodeView(String id, String httpUrl, String raftAddress, boolean reachable, String health, String role,
                       long term, String leaderId, String votedFor, long commitIndex, long lastApplied,
                       long lastLogIndex, long lastLogTerm, long snapshotIndex, int pendingProposals, String failure,
                       long uptimeMs, Map<String, Follower> followers, Map<String, GrpcTransport.PeerView> links,
                       Faults faults, StorageView storage, long observedAtMs) {

    /** Leader's view of a follower; {@code lag} = leader's last log index minus follower's match index. */
    public record Follower(long nextIndex, long matchIndex, long lag, long lastAckAgoMs) {}

    public record Faults(List<String> blockedPeers, long delayMs, double dropProbability, boolean storageFailing,
                         boolean crashed, List<String> active) {}

    public record StorageView(long diskBytes, long snapshotIndex, long lastIndex, int keys, boolean fsync) {}

    public static NodeView unreachable(String id, String httpUrl, String raftAddress, String why) {
        return new NodeView(id, httpUrl, raftAddress, false, "UNREACHABLE", "UNKNOWN", 0, null, null, 0, 0, 0, 0, 0, 0,
                why, 0, Map.of(), Map.of(), null, null, System.currentTimeMillis());
    }
}
