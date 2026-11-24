package io.quorumdb.raft;

import java.util.List;

/**
 * Messages exchanged between Raft peers. The engine is message-driven: requests and replies
 * are independent one-way messages, so the transport may drop, delay, duplicate or reorder
 * any of them and every handler must stay correct (Raft §5.5: RPCs are idempotent).
 */
public sealed interface RaftMessage {

    /** Sender node id. */
    String from();

    /** Sender's current term (for pre-vote requests: the term it would campaign in). */
    long term();

    /** RequestVote RPC (Raft §5.2, §5.4.1). {@code preVote} probes electability without disturbing terms. */
    record RequestVote(String from, long term, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements RaftMessage {}

    /**
     * @param term        the voter's current term
     * @param requestTerm echo of the request's term so late replies from older rounds are ignored
     */
    record RequestVoteReply(String from, long term, long requestTerm, boolean granted, boolean preVote)
            implements RaftMessage {}

    /**
     * AppendEntries RPC (Raft §5.3); empty {@code entries} is a heartbeat.
     *
     * @param readSeq leader-local sequence echoed by the reply; used to confirm leadership for
     *                ReadIndex linearizable reads (only replies to messages sent after the read count)
     */
    record AppendEntries(String from, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit, long readSeq) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * Reply to AppendEntries and InstallSnapshot.
     *
     * @param matchIndex    on success: highest index known to match the leader's log
     * @param conflictIndex on failure: where the leader should back up to (fast log backtracking)
     * @param conflictTerm  on failure: term of the conflicting entry, or 0 if the follower's log is too short
     */
    record AppendEntriesReply(String from, long term, boolean success, long matchIndex,
                              long conflictIndex, long conflictTerm, long readSeq) implements RaftMessage {}

    /**
     * One chunk of an InstallSnapshot RPC (Raft §7, Figure 13).
     *
     * @param offset byte offset of {@code data} within the snapshot
     * @param more   true if further chunks follow; false for the last (or only) chunk
     */
    record InstallSnapshot(String from, long term, long lastIncludedIndex, long lastIncludedTerm,
                           long offset, byte[] data, boolean more, long readSeq) implements RaftMessage {

        /** The whole snapshot in one message. */
        public InstallSnapshot(String from, long term, long lastIncludedIndex, long lastIncludedTerm,
                               byte[] data, long readSeq) {
            this(from, term, lastIncludedIndex, lastIncludedTerm, 0, data, false, readSeq);
        }
    }

    /**
     * Acknowledges a non-final InstallSnapshot chunk. The last chunk is answered with an
     * {@link AppendEntriesReply} instead, as for a snapshot sent in one message.
     *
     * @param nextOffset bytes of the snapshot at {@code lastIncludedIndex} received so far
     */
    record InstallSnapshotReply(String from, long term, long lastIncludedIndex, long nextOffset, long readSeq)
            implements RaftMessage {}
}
