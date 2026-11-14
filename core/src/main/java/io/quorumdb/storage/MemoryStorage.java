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
    @Override public long lastIndex() { return snapshotIndex + log.size(); }

    @Override
    public long termAt(long index) {
        if (index == 0) return 0;
        if (index == snapshotIndex) return snapshotTerm;
        return entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("index " + index + " outside retained log ("
                    + snapshotIndex + ", " + lastIndex() + "]");
        }
        return log.get((int) (index - snapshotIndex - 1));
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndexInclusive) {
        if (fromIndex > toIndexInclusive) return List.of();
        entry(fromIndex);
        entry(toIndexInclusive);
        return List.copyOf(log.subList((int) (fromIndex - snapshotIndex - 1), (int) (toIndexInclusive - snapshotIndex)));
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
    public void saveSnapshot(long index, long term, byte[] data) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("snapshot index " + index + " not in log");
        }
        log.subList(0, (int) (index - snapshotIndex)).clear();
        setSnapshot(index, term, data);
    }

    @Override
    public void installSnapshot(long index, long term, byte[] data) {
        boolean keepSuffix = index > snapshotIndex && index <= lastIndex() && termAt(index) == term;
        if (keepSuffix) {
            log.subList(0, (int) (index - snapshotIndex)).clear();
        } else {
            log.clear();
        }
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
        log.clear();
    }

    @Override public void close() {}
}
