package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;

import java.util.List;

/**
 * Durable Raft state: hard state (term, vote), the log and the latest snapshot.
 *
 * <p>Contract: every mutating method except {@link #append} returns only after the change is durable
 * (to the degree the implementation's sync policy promises). {@code append} may return before the
 * entries are durable; {@link #durableIndex()} says how far the log is durable, and the node acknowledges
 * nothing above it. Raft's safety argument depends on this: a node must never forget a vote it cast or
 * an entry it acknowledged. Implementations signal I/O failure with {@link StorageException}; the node
 * then fail-stops rather than continue on unknown state.
 *
 * <p>Index model: the snapshot covers indexes {@code [1, snapshotIndex]}; the log holds the entries
 * {@code [firstIndex, lastIndex]}, where {@code firstIndex <= snapshotIndex + 1}. Entries at or before
 * {@code snapshotIndex} are an optional tail kept after a local snapshot; they are committed and match
 * the snapshot. Not thread-safe; owned by the node thread.
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

    /** Index of the first retained log entry; {@code snapshotIndex() + 1} when no tail is kept. */
    default long firstIndex() {
        return snapshotIndex() + 1;
    }

    /** Index of the last log entry, or {@link #snapshotIndex()} if the log is empty. */
    long lastIndex();

    /**
     * @return term of the entry at {@code index} in {@code [firstIndex, lastIndex]}; works for
     *         {@code snapshotIndex} too; 0 for index 0
     * @throws IllegalArgumentException if the index is compacted away or beyond the log
     */
    long termAt(long index);

    LogEntry entry(long index);

    /** Entries in {@code [fromIndex, toIndexInclusive]}; both must be within the retained log. */
    List<LogEntry> entries(long fromIndex, long toIndexInclusive);

    /**
     * Appends entries; the first must have index {@code lastIndex() + 1} and they must be contiguous. The
     * entries are readable at once but may become durable later (see {@link #durableIndex()}).
     */
    void append(List<LogEntry> entries);

    /**
     * Highest log index that is durable (at least {@link #snapshotIndex()}). Entries above it are readable
     * but would be lost in a crash, so they must not be acknowledged yet.
     *
     * @throws StorageException if a background write failed
     */
    default long durableIndex() {
        return lastIndex();
    }

    /**
     * Registers a callback run, on a storage thread, after {@link #durableIndex()} advances or a background
     * write fails. Implementations that are durable on return from {@link #append} never call it.
     */
    default void setDurabilityListener(Runnable listener) {}

    /** Removes every entry with index {@code >= fromIndex} (conflict resolution, Raft §5.3). */
    void truncateSuffix(long fromIndex);

    /**
     * Saves a snapshot taken locally at {@code index} (which must be in the log) and compacts the
     * log prefix up to and including it.
     */
    default void saveSnapshot(long index, long term, byte[] data) {
        saveSnapshot(index, term, data, 0);
    }

    /**
     * Saves a snapshot taken locally at {@code index} (which must be in the log) and compacts the log
     * prefix, keeping the last {@code trailingEntries} entries at or before {@code index}.
     */
    void saveSnapshot(long index, long term, byte[] data, long trailingEntries);

    /**
     * Installs a snapshot received from the leader. Retains the log suffix after {@code index} only if
     * the log contains a matching entry at {@code index}; otherwise the whole log is discarded. No tail
     * at or before {@code index} is kept.
     */
    void installSnapshot(long index, long term, byte[] data);

    /** Bytes currently used on stable storage (0 for in-memory implementations). */
    default long diskUsageBytes() {
        return 0;
    }

    @Override
    void close();
}
