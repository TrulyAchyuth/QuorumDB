package io.quorumdb.raft;

import io.quorumdb.storage.RaftStorage;

import java.util.List;

/** Test decorator: while held, appended entries stay non-durable, as if the background fsync were slow. */
final class LaggingStorage implements RaftStorage {
    private final RaftStorage delegate;
    private long heldAt = -1; // durable index frozen while held; -1 = not held

    LaggingStorage(RaftStorage delegate) {
        this.delegate = delegate;
    }

    void hold() {
        heldAt = delegate.durableIndex();
    }

    void release() {
        heldAt = -1;
    }

    @Override
    public long durableIndex() {
        long durable = delegate.durableIndex();
        return heldAt < 0 ? durable : Math.max(Math.min(heldAt, durable), delegate.snapshotIndex());
    }

    @Override public long currentTerm() { return delegate.currentTerm(); }
    @Override public String votedFor() { return delegate.votedFor(); }
    @Override public void saveHardState(long term, String votedFor) { delegate.saveHardState(term, votedFor); }
    @Override public long snapshotIndex() { return delegate.snapshotIndex(); }
    @Override public long snapshotTerm() { return delegate.snapshotTerm(); }
    @Override public byte[] loadSnapshot() { return delegate.loadSnapshot(); }
    @Override public long lastIndex() { return delegate.lastIndex(); }
    @Override public long termAt(long index) { return delegate.termAt(index); }
    @Override public LogEntry entry(long index) { return delegate.entry(index); }
    @Override public List<LogEntry> entries(long from, long to) { return delegate.entries(from, to); }
    @Override public void append(List<LogEntry> entries) { delegate.append(entries); }
    @Override public void truncateSuffix(long fromIndex) { delegate.truncateSuffix(fromIndex); }
    @Override public void saveSnapshot(long index, long term, byte[] data) { delegate.saveSnapshot(index, term, data); }
    @Override public void installSnapshot(long index, long term, byte[] data) { delegate.installSnapshot(index, term, data); }
    @Override public void close() { delegate.close(); }
}
