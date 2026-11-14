package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;

import java.util.List;

/**
 * Decorator that lets FaultLab make stable storage fail on demand. While failing, every write
 * throws {@link StorageException}, which the node treats as fatal (fail-stop). Reads keep working
 * so the node can still be inspected.
 */
public final class FaultyStorage implements RaftStorage {
    private final RaftStorage delegate;
    private volatile boolean failWrites;

    public FaultyStorage(RaftStorage delegate) {
        this.delegate = delegate;
    }

    public void setFailWrites(boolean failWrites) {
        this.failWrites = failWrites;
    }

    public boolean isFailingWrites() {
        return failWrites;
    }

    public RaftStorage delegate() {
        return delegate;
    }

    private void checkWritable(String op) {
        if (failWrites) throw new StorageException("injected storage failure during " + op);
    }

    @Override public long currentTerm() { return delegate.currentTerm(); }
    @Override public String votedFor() { return delegate.votedFor(); }
    @Override public long snapshotIndex() { return delegate.snapshotIndex(); }
    @Override public long snapshotTerm() { return delegate.snapshotTerm(); }
    @Override public byte[] loadSnapshot() { return delegate.loadSnapshot(); }
    @Override public long lastIndex() { return delegate.lastIndex(); }
    @Override public long termAt(long index) { return delegate.termAt(index); }
    @Override public LogEntry entry(long index) { return delegate.entry(index); }
    @Override public List<LogEntry> entries(long from, long to) { return delegate.entries(from, to); }
    @Override public long diskUsageBytes() { return delegate.diskUsageBytes(); }

    @Override
    public void saveHardState(long term, String votedFor) {
        checkWritable("saveHardState");
        delegate.saveHardState(term, votedFor);
    }

    @Override
    public void append(List<LogEntry> entries) {
        checkWritable("append");
        delegate.append(entries);
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        checkWritable("truncateSuffix");
        delegate.truncateSuffix(fromIndex);
    }

    @Override
    public void saveSnapshot(long index, long term, byte[] data) {
        checkWritable("saveSnapshot");
        delegate.saveSnapshot(index, term, data);
    }

    @Override
    public void installSnapshot(long index, long term, byte[] data) {
        checkWritable("installSnapshot");
        delegate.installSnapshot(index, term, data);
    }

    @Override public void close() { delegate.close(); }
}
