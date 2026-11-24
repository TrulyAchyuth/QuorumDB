package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory {@link RaftStorage}. In the simulator one instance outlives the "process" it belongs
 * to, modelling a disk that survives crashes; {@link #wipe()} models losing that disk.
 */
public final class MemoryStorage implements RaftStorage {
    private long term;
    private String votedFor;
    private long snapshotIndex;
    private long snapshotTerm;
    private byte[] snapshot;
    private long firstIndex = 1; // index of log.get(0)
    private final ArrayList<LogEntry> log = new ArrayList<>();

    @Override public long currentTerm() { return term; }
    @Override public String votedFor() { return votedFor; }

    @Override
    public void saveHardState(long term, String votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override public long snapshotIndex() { return snapshotIndex; }
    @Override public long snapshotTerm() { return snapshotTerm; }
    @Override public byte[] loadSnapshot() { return snapshot; }
    @Override public long firstIndex() { return firstIndex; }
    @Override public long lastIndex() { return firstIndex + log.size() - 1; }

    @Override
    public long termAt(long index) {
        if (index == 0) return 0;
        if (index == snapshotIndex) return snapshotTerm;
        return entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        if (index < firstIndex || index > lastIndex()) {
            throw new IllegalArgumentException("index " + index + " outside retained log ["
                    + firstIndex + ", " + lastIndex() + "]");
        }
        return log.get((int) (index - firstIndex));
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndexInclusive) {
        if (fromIndex > toIndexInclusive) return List.of();
        entry(fromIndex);
        entry(toIndexInclusive);
        return List.copyOf(log.subList((int) (fromIndex - firstIndex), (int) (toIndexInclusive - firstIndex + 1)));
    }

    @Override
    public void append(List<LogEntry> entries) {
        long expected = lastIndex() + 1;
        for (LogEntry e : entries) {
            if (e.index() != expected++) throw new IllegalArgumentException("non-contiguous append at " + e.index());
        }
        log.addAll(entries);
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        if (fromIndex <= snapshotIndex) throw new IllegalArgumentException("cannot truncate into the snapshot");
        while (lastIndex() >= fromIndex) log.remove(log.size() - 1);
    }

    @Override
    public void saveSnapshot(long index, long term, byte[] data, long trailingEntries) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("snapshot index " + index + " not in log");
        }
        long keepFrom = Math.max(firstIndex, index + 1 - trailingEntries);
        log.subList(0, (int) (keepFrom - firstIndex)).clear();
        firstIndex = keepFrom;
        setSnapshot(index, term, data);
    }

    @Override
    public void installSnapshot(long index, long term, byte[] data) {
        boolean keepSuffix = index > snapshotIndex && index <= lastIndex() && termAt(index) == term;
        if (keepSuffix) {
            log.subList(0, (int) (index + 1 - firstIndex)).clear();
        } else {
            log.clear();
        }
        firstIndex = index + 1;
        setSnapshot(index, term, data);
    }

    private void setSnapshot(long index, long term, byte[] data) {
        this.snapshotIndex = index;
        this.snapshotTerm = term;
        this.snapshot = data.clone();
    }

    /** Simulates total loss of the disk (violates Raft's durability assumption; used by FaultLab on purpose). */
    public void wipe() {
        term = 0;
        votedFor = null;
        snapshotIndex = 0;
        snapshotTerm = 0;
        snapshot = null;
        firstIndex = 1;
        log.clear();
    }

    @Override public void close() {}
}
