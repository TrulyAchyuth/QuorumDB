package io.quorumdb.raft;

/**
 * A structured record of something that happened on a node. Events are the raw evidence used by
 * the FaultLab timeline, the safety checker and the explanation generator.
 *
 * @param seq       per-emitter sequence number (gap-free; lets consumers detect dropped observations)
 * @param timeMs    emitter clock (virtual time in simulation, wall clock in a live cluster)
 * @param node      node the event happened on (or "faultlab"/"client-N" for injected/client events)
 * @param term      the node's current term when the event was emitted (0 if not applicable)
 * @param index     log index the event refers to (0 if not applicable)
 * @param entryTerm term of the log entry at {@code index} (0 if not applicable)
 * @param checksum  {@link LogEntry#checksum()} of that entry (0 if not applicable)
 * @param peer      the other node involved, if any
 * @param detail    short human-readable context
 */
public record RaftEvent(long seq, long timeMs, String node, Type type, long term, long index,
                        long entryTerm, long checksum, String peer, String detail) {

    public enum Type {
        // lifecycle
        NODE_STARTED, RECOVERY_COMPLETED, NODE_STOPPED, NODE_FAILED,
        // elections
        ELECTION_TIMEOUT, PRE_VOTE_STARTED, ELECTION_STARTED, VOTE_REQUESTED, VOTE_GRANTED, VOTE_DENIED,
        LEADER_ELECTED, STEPPED_DOWN, TERM_ADVANCED,
        // replication
        HEARTBEAT_SENT, ENTRY_APPENDED, LOG_TRUNCATED, ENTRY_COMMITTED, ENTRY_APPLIED,
        // snapshots
        SNAPSHOT_CREATED, SNAPSHOT_SENT, SNAPSHOT_INSTALLED,
        // emitted by FaultLab (simulator or live fault controller), never by the Raft core
        MESSAGE_SENT, MESSAGE_DELIVERED, MESSAGE_DROPPED,
        NODE_CRASHED, NODE_RESTARTED, NODE_DISCONNECTED, NODE_RECOVERED,
        FAULT_INJECTED, FAULT_HEALED,
        CLIENT_REQUEST, CLIENT_RESPONSE
    }
}
