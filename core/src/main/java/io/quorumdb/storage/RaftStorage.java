package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;

import java.util.List;

/**
 * Durable Raft state: hard state (term, vote), the log and the latest snapshot.
 *
 * <p>Contract: every mutating method returns only after the change is durable (to the degree the
 * implementation's sync policy promises). Raft's safety argument depends on this: a node must
 * never forget a vote it cast or an entry it acknowledged. Implementations signal I/O failure
 * with {@link StorageException}; the node then fail-stops rather than continue on unknown state.
 *
 * <p>Index model: the snapshot covers indexes {@code [1, snapshotIndex]}; the log holds exactly
 * the entries {@code (snapshotIndex, lastIndex]}. Not thread-safe; owned by the node thread.
 */
public interface RaftStorage extends AutoCloseable {

    long currentTerm();

    /** @return id of the candidate voted for in {@link #currentTerm()}, or null */
    String votedFor();

    void saveHardState(long term, String votedFor);

    /** Index of the last entry covered by the snapshot (0 if none). */
    long snapshotIndex();

    long snapshotTerm();

    /** @return the snapshot payload, or null if no snapshot exists */
    byte[] loadSnapshot();

    /** Index of the last log entry, or {@link #snapshotIndex()} if the log is empty. */
    long lastIndex();

    /**
     * @return term of the entry at {@code index}; works for {@code snapshotIndex} too; 0 for index 0
     * @throws IllegalArgumentException if the index is compacted away or beyond the log
     */
    long termAt(long index);

    LogEntry entry(long index);

    /** Entries in {@code [fromIndex, toIndexInclusive]}; both must be within the retained log. */
    List<LogEntry> entries(long fromIndex, long toIndexInclusive);

    /** Appends entries; the first must have index {@code lastIndex() + 1} and they must be contiguous. */
    void append(List<LogEntry> entries);

    /** Removes every entry with index {@code >= fromIndex} (conflict resolution, Raft §5.3). */
    void truncateSuffix(long fromIndex);

    /**
     * Saves a snapshot taken locally at {@code index} (which must be in the log) and compacts the
     * log prefix up to and including it.
     */
    void saveSnapshot(long index, long term, byte[] data);

    /**
     * Installs a snapshot received from the leader. Retains the log suffix after {@code index} only if
     * the log contains a matching entry at {@code index}; otherwise the whole log is discarded.
     */
    void installSnapshot(long index, long term, byte[] data);

    /** Bytes currently used on stable storage (0 for in-memory implementations). */
    default long diskUsageBytes() {
        return 0;
    }

    @Override
    void close();
}
