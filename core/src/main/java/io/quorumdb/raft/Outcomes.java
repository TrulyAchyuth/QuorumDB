package io.quorumdb.raft;

/** Results delivered to callers of {@link RaftNode#propose} and {@link RaftNode#linearizableRead}. */
public final class Outcomes {
    private Outcomes() {}

    public enum Status {
        /** The entry was committed by a majority and applied on this leader. */
        OK,
        /** Definitely not executed: this node is not the leader. Safe to retry elsewhere. */
        NOT_LEADER,
        /** Definitely not executed: the leader's proposal queue is full. */
        BUSY,
        /**
         * The entry was appended but this node lost leadership (or stopped) before learning its fate.
         * It MAY still be committed by a later leader: the outcome is unknown, not a failure.
         */
        UNKNOWN
    }

    /**
     * @param index      log index assigned to the command (0 unless it was appended)
     * @param result     state machine result (only for {@link Status#OK})
     * @param leaderHint best known leader id, may be null
     */
    public record Proposal(Status status, long index, long term, byte[] result, String leaderHint) {}

    /**
     * @param readIndex the state machine has applied at least this index; reading it now is linearizable
     */
    public record Read(Status status, long readIndex, String leaderHint) {}
}
