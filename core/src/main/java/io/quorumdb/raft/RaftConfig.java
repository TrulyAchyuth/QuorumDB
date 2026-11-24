package io.quorumdb.raft;

import java.util.List;

/**
 * Static configuration of one Raft node.
 *
 * @param nodeId                 this node's id
 * @param peers                  ids of the <em>other</em> voting members (fixed membership)
 * @param electionTimeoutMinMs   lower bound of the randomized election timeout
 * @param electionTimeoutMaxMs   upper bound (exclusive) of the randomized election timeout
 * @param heartbeatIntervalMs    leader heartbeat period; must be well below the election timeout
 * @param maxEntriesPerAppend    cap on entries per AppendEntries message
 * @param maxBytesPerAppend      soft cap on payload bytes per AppendEntries message
 * @param snapshotThreshold      take a snapshot after this many applied entries since the last one (0 = never)
 * @param maxPendingProposals    back-pressure limit on uncommitted client proposals held by a leader
 * @param preVote                run a non-disruptive pre-vote round before real elections (Ongaro thesis §9.6)
 * @param checkQuorum            leader steps down when it has not heard from a majority for an election timeout
 * @param emitHeartbeatEvents    whether HEARTBEAT_SENT events are emitted (noisy on real clusters)
 * @param joining                this node is being added to a running cluster: {@code peers} are the current
 *                               members and this node is not a voter until a configuration entry adds it
 */
public record RaftConfig(String nodeId, List<String> peers,
                         long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs,
                         int maxEntriesPerAppend, int maxBytesPerAppend, long snapshotThreshold,
                         int maxPendingProposals, boolean preVote, boolean checkQuorum,
                         boolean emitHeartbeatEvents, boolean joining) {

    public RaftConfig(String nodeId, List<String> peers,
                      long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs,
                      int maxEntriesPerAppend, int maxBytesPerAppend, long snapshotThreshold,
                      int maxPendingProposals, boolean preVote, boolean checkQuorum, boolean emitHeartbeatEvents) {
        this(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, maxEntriesPerAppend,
                maxBytesPerAppend, snapshotThreshold, maxPendingProposals, preVote, checkQuorum, emitHeartbeatEvents, false);
    }

    public RaftConfig {
        if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("nodeId is required");
        peers = List.copyOf(peers);
        if (peers.contains(nodeId)) throw new IllegalArgumentException("peers must not contain the node itself");
        if (peers.stream().distinct().count() != peers.size()) throw new IllegalArgumentException("duplicate peer id");
        if (electionTimeoutMinMs <= 0 || electionTimeoutMaxMs <= electionTimeoutMinMs) {
            throw new IllegalArgumentException("election timeout range must satisfy 0 < min < max");
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs * 2 > electionTimeoutMinMs) {
            throw new IllegalArgumentException("heartbeat interval must be at most half the minimum election timeout");
        }
        if (maxEntriesPerAppend < 1 || maxBytesPerAppend < 1 || maxPendingProposals < 1 || snapshotThreshold < 0) {
            throw new IllegalArgumentException("batching/snapshot limits must be positive");
        }
    }

    public static RaftConfig defaults(String nodeId, List<String> peers) {
        return new RaftConfig(nodeId, peers, 150, 300, 50, 512, 1 << 20, 10_000, 10_000, true, true, true);
    }

    /** Size of the initial (static) configuration. */
    public int clusterSize() {
        return peers.size() + 1;
    }

    /** Smallest number of nodes that forms a majority of the initial configuration. */
    public int quorum() {
        return clusterSize() / 2 + 1;
    }

    /** Voting members this node starts with, before any configuration entry: itself and its peers, or only the peers if joining. */
    public List<String> initialMembers() {
        List<String> out = new java.util.ArrayList<>();
        if (!joining) out.add(nodeId);
        out.addAll(peers);
        return List.copyOf(out);
    }

    public RaftConfig withJoining(boolean value) {
        return new RaftConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, maxBytesPerAppend, snapshotThreshold, maxPendingProposals, preVote, checkQuorum,
                emitHeartbeatEvents, value);
    }

    public RaftConfig withPreVote(boolean value) {
        return new RaftConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, maxBytesPerAppend, snapshotThreshold, maxPendingProposals, value, checkQuorum,
                emitHeartbeatEvents, joining);
    }

    public RaftConfig withSnapshotThreshold(long value) {
        return new RaftConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, maxBytesPerAppend, value, maxPendingProposals, preVote, checkQuorum,
                emitHeartbeatEvents, joining);
    }

    public RaftConfig withTimings(long electionMinMs, long electionMaxMs, long heartbeatMs) {
        return new RaftConfig(nodeId, peers, electionMinMs, electionMaxMs, heartbeatMs,
                maxEntriesPerAppend, maxBytesPerAppend, snapshotThreshold, maxPendingProposals, preVote, checkQuorum,
                emitHeartbeatEvents, joining);
    }

    public RaftConfig withCheckQuorum(boolean value) {
        return new RaftConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, maxBytesPerAppend, snapshotThreshold, maxPendingProposals, preVote, value,
                emitHeartbeatEvents, joining);
    }

    public RaftConfig withHeartbeatEvents(boolean value) {
        return new RaftConfig(nodeId, peers, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, maxBytesPerAppend, snapshotThreshold, maxPendingProposals, preVote, checkQuorum,
                value, joining);
    }
}
