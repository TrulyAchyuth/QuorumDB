package io.quorumdb.raft;

import java.util.Map;

/** Point-in-time view of a node, safe to hand to other threads. */
public record RaftStatus(String nodeId, RaftNode.Role role, long term, String leaderId, String votedFor,
                         long commitIndex, long lastApplied, long lastLogIndex, long lastLogTerm,
                         long snapshotIndex, long snapshotTerm, boolean running, String failure,
                         int pendingProposals, Map<String, Peer> peers) {

    /**
     * Leader's view of a follower (only populated on the leader).
     *
     * @param lastAckAgoMs milliseconds since the follower last acknowledged this leader, -1 if never
     */
    public record Peer(long nextIndex, long matchIndex, long lastAckAgoMs) {}
}
