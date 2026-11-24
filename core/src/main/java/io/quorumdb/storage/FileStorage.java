package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/**
 * File-backed {@link RaftStorage}.
 *
 * <pre>
 * dir/
 *   LOCK                              exclusive process lock
 *   hardstate                         term + vote; replaced atomically (tmp, fsync, rename, fsync dir)
 *   wal/00000000000000000001.wal      write-ahead log segments, named by the index of their first record
 *   snapshots/00000000000000000042-3.snap   snapshot at index 42, term 3; written atomically
 * </pre>
 *
 * <h2>WAL format</h2>
 * Segment: {@code "QDBWAL1\n"} + first index (8 bytes), followed by records
 * {@code [length:4][crc32c(payload):4][payload]} where payload is
 * {@code [index:8][term:8][type:1][data]}. Records are only ever appended; conflict truncation
 * shortens the file.
 *
 * <h2>Recovery</h2>
 * Every record is CRC-checked. An invalid record at the very end of the newest segment with no
 * valid record after it is a <em>torn write</em> from a crash mid-append: it was never acknowledged,
 * so it is discarded. Anything else (a bad record followed by valid ones, damage in an older
 * segment, a gap between snapshot and log) is <em>corruption</em> and aborts startup with
 * {@link StorageException.Corrupted}, because silently dropping acknowledged entries could break
 * Raft's safety guarantees.
 *
 * <h2>Background WAL writes</h2>
 * {@link #append} updates the in-memory log and hands the record write to a single writer thread, so the
 * Raft thread does not wait for the disk. The writer fsyncs once its queue is empty (one fsync covers every
 * append queued behind the one in progress), then advances {@link #durableIndex()} and runs the durability
 * listener. A failed background write is remembered and rethrown by the next call on the Raft thread.
 * Every other mutation first waits for queued writes, then runs synchronously, so the files see the same
 * order of operations as before.
 *
 * <h2>Durability assumptions</h2>
 * With {@link SyncMode#ALWAYS} each mutating call ends with {@code fsync} (FileChannel.force), so
 * the guarantee is exactly as strong as the OS/device honouring fsync. {@link SyncMode#NEVER}
 * leaves flushing to the OS: data survives a process crash but not necessarily power loss, and a
 * node that loses acknowledged state can violate safety. It exists for benchmarking only.
 */
public final class FileStorage implements RaftStorage {

    public enum SyncMode { ALWAYS, NEVER }

    private static final byte[] WAL_MAGIC = "QDBWAL1\n".getBytes(StandardCharsets.US_ASCII);
    private static final int SEGMENT_HEADER = WAL_MAGIC.length + 8;
    private static final int RECORD_HEADER = 8;
    private static final int MIN_PAYLOAD = 17;
    private static final int MAX_PAYLOAD = 64 << 20;
    private static final int HARD_STATE_MAGIC = 0x51444248; // "QDBH"
    private static final int SNAPSHOT_MAGIC = 0x51444246;   // "QDBF"

    private static final class Segment {
        final Path path;
        final long firstIndex;
        FileChannel channel;
        long size;
        long nextIndex;

        Segment(Path path, long firstIndex) {
            this.path = path;
            this.firstIndex = firstIndex;
            this.nextIndex = firstIndex;
        }
    }

    private final Path dir;
    private final Path walDir;
    private final Path snapshotDir;
    private final SyncMode syncMode;
    private final long segmentBytes;
    private final FileChannel lockChannel;
    private final FileLock lock;

    private long term;
    private String votedFor;
    private long snapshotIndex;
    private long snapshotTerm;
    private Path snapshotPath;

    /** Retained entries, i.e. indexes [firstIndex, lastIndex]; {@code offsets} is parallel. */
    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private long firstIndex = 1;
    private final ArrayList<Long> offsets = new ArrayList<>();
    private final ArrayList<Segment> segments = new ArrayList<>();
    private boolean closed;

    private final ThreadPoolExecutor writer;
    /** Highest index written to the WAL by the writer (written by the writer, or by the Raft thread after {@link #drain()}). */
    private long writtenIndex;
    private volatile long durableIndex;
    private volatile StorageException writeFailure;
    private volatile Runnable durabilityListener = () -> { };

    public FileStorage(Path dir, SyncMode syncMode, long segmentBytes) {
        this.dir = dir;
        this.walDir = dir.resolve("wal");
        this.snapshotDir = dir.resolve("snapshots");
        this.syncMode = syncMode;
        this.segmentBytes = segmentBytes;
        this.writer = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r, "wal-writer-" + dir.getFileName());
            t.setDaemon(true);
            return t;
        });
        try {
            Files.createDirectories(walDir);
            Files.createDirectories(snapshotDir);
            lockChannel = FileChannel.open(dir.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            lock = lockChannel.tryLock();
            if (lock == null) {
                lockChannel.close();
                throw new StorageException("data directory " + dir + " is in use by another process");
            }
            recover();
            writtenIndex = lastIndex();
            durableIndex = writtenIndex;
        } catch (IOException e) {
            throw new StorageException("cannot open storage at " + dir, e);
        } catch (java.nio.channels.OverlappingFileLockException e) {
            throw new StorageException("data directory " + dir + " is already open in this process");
        }
    }

    public FileStorage(Path dir) {
        this(dir, SyncMode.ALWAYS, 8L << 20);
    }

    // =====================================================================================
    // Recovery
    // =====================================================================================

    private void recover() throws IOException {
        for (Path d : List.of(dir, walDir, snapshotDir)) {
            try (Stream<Path> files = Files.list(d)) {
                for (Path tmp : files.filter(p -> p.getFileName().toString().endsWith(".tmp")).toList()) {
                    Files.delete(tmp); // leftovers of an interrupted atomic write; never referenced
                }
            }
        }
        recoverHardState();
        recoverSnapshot();
        recoverLog();
    }

    private void recoverHardState() throws IOException {
        Path file = dir.resolve("hardstate");
        if (!Files.exists(file)) return;
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file));
        try {
            if (buf.getInt() != HARD_STATE_MAGIC) throw new StorageException.Corrupted("hardstate: bad magic");
            long t = buf.getLong();
            int voteLength = buf.getInt();
            byte[] vote = new byte[Math.max(voteLength, 0)];
            buf.get(vote);
            int crc = buf.getInt();
            CRC32C check = new CRC32C();
            check.update(buf.array(), 0, buf.position() - 4);
            if ((int) check.getValue() != crc) throw new StorageException.Corrupted("hardstate: checksum mismatch");
            term = t;
            votedFor = voteLength < 0 ? null : new String(vote, StandardCharsets.UTF_8);
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
            throw new StorageException.Corrupted("hardstate: truncated");
        }
    }

    private void recoverSnapshot() throws IOException {
        List<Path> candidates;
        try (Stream<Path> files = Files.list(snapshotDir)) {
            candidates = files.filter(p -> p.getFileName().toString().endsWith(".snap"))
                    .sorted(Comparator.comparing((Path p) -> p.getFileName().toString()).reversed()).toList();
        }
        for (Path candidate : candidates) {
            ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(candidate));
            if (buf.remaining() < 28 || buf.getInt() != SNAPSHOT_MAGIC) continue;
            long index = buf.getLong();
            long t = buf.getLong();
            int length = buf.getInt();
            int crc = buf.getInt();
            if (length != buf.remaining()) continue;
            CRC32C check = new CRC32C();
            check.update(buf.array(), buf.position(), length);
            if ((int) check.getValue() != crc) continue;
            snapshotIndex = index;
            snapshotTerm = t;
            snapshotPath = candidate;
            return;
            // An unreadable newer snapshot is skipped; if the log it replaced is gone, the gap
            // check in recoverLog() reports corruption instead of serving an inconsistent state.
        }
    }

    private void recoverLog() throws IOException {
        List<Path> files;
        try (Stream<Path> list = Files.list(walDir)) {
            files = list.filter(p -> p.getFileName().toString().endsWith(".wal")).sorted().toList();
        }
        long expected = -1; // next index we expect to read; -1 = nothing read yet
        for (int f = 0; f < files.size(); f++) {
            Path path = files.get(f);
            boolean isLast = f == files.size() - 1;
            ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(path));
            long nameIndex = Long.parseLong(path.getFileName().toString().replace(".wal", ""));
            if (buf.remaining() < SEGMENT_HEADER) {
                if (isLast) { // crashed while creating the segment; nothing in it was acknowledged
                    Files.delete(path);
                    continue;
                }
                throw new StorageException.Corrupted(path + ": truncated segment header");
            }
            byte[] magic = new byte[WAL_MAGIC.length];
            buf.get(magic);
            long firstIndex = buf.getLong();
            if (!java.util.Arrays.equals(magic, WAL_MAGIC) || firstIndex != nameIndex) {
                throw new StorageException.Corrupted(path + ": bad segment header");
            }
            if (expected != -1 && firstIndex != expected && !(firstIndex > expected && firstIndex <= snapshotIndex + 1)) {
                throw new StorageException.Corrupted(path + ": expected log to continue at " + expected);
            }
            Segment segment = new Segment(path, firstIndex);
            if (firstIndex != expected) {
                // A gap allowed above lies within the snapshot: what was read before it cannot be a contiguous tail.
                entries.clear();
                offsets.clear();
            }
            expected = firstIndex;
            long validEnd = SEGMENT_HEADER;
            while (buf.hasRemaining()) {
                int start = buf.position();
                LogEntry e = readRecord(buf);
                if (e == null || e.index() != expected) {
                    if (!isLast || validRecordAfter(buf.array(), start + 1, expected)) {
                        throw new StorageException.Corrupted(path + ": invalid record at offset " + start
                                + " followed by valid data (expected index " + expected + ")");
                    }
                    break; // torn tail
                }
                entries.add(e);
                offsets.add((long) start);
                expected++;
                validEnd = buf.position();
            }
            segment.nextIndex = expected;
            segment.size = validEnd;
            segment.channel = FileChannel.open(path, StandardOpenOption.WRITE);
            if (validEnd < buf.capacity()) {
                segment.channel.truncate(validEnd);
                segment.channel.force(true);
            }
            segment.channel.position(validEnd);
            segments.add(segment);
        }
        if (!entries.isEmpty() && entries.get(0).index() > snapshotIndex + 1) {
            throw new StorageException.Corrupted("log starts at " + entries.get(0).index()
                    + " but snapshot ends at " + snapshotIndex + ": entries are missing");
        }
        firstIndex = entries.isEmpty() ? snapshotIndex + 1 : entries.get(0).index();
        // Entries at or before the snapshot are kept as a tail only if they provably match it (same term at the
        // snapshot index, so the whole prefix matches by Log Matching). A log left behind by a crash during
        // installSnapshot may not; then they are dropped, as they always were before tails existed.
        long last = firstIndex + entries.size() - 1;
        boolean tailMatches = firstIndex > snapshotIndex
                || (last >= snapshotIndex && entries.get((int) (snapshotIndex - firstIndex)).term() == snapshotTerm);
        if (!tailMatches) {
            int drop = (int) Math.min(entries.size(), snapshotIndex + 1 - firstIndex);
            entries.subList(0, drop).clear();
            offsets.subList(0, drop).clear();
            firstIndex = snapshotIndex + 1;
        }
    }

    /** Parses one record at the buffer's position; returns null (position undefined) if invalid. */
    private static LogEntry readRecord(ByteBuffer buf) {
        if (buf.remaining() < RECORD_HEADER) return null;
        int length = buf.getInt();
        int crc = buf.getInt();
        if (length < MIN_PAYLOAD || length > MAX_PAYLOAD || length > buf.remaining()) return null;
        CRC32C check = new CRC32C();
        check.update(buf.array(), buf.position(), length);
        if ((int) check.getValue() != crc) return null;
        long index = buf.getLong();
        long term = buf.getLong();
        int type = buf.get();
        byte[] data = new byte[length - MIN_PAYLOAD];
        buf.get(data);
        if (index < 1 || term < 1 || type < 0 || type >= LogEntry.Type.values().length) return null;
        return new LogEntry(index, term, LogEntry.Type.fromOrdinal(type), data);
    }

    /** Is there any intact record with a plausible index after {@code from}? Then damage is not a torn tail. */
    private static boolean validRecordAfter(byte[] file, int from, long expectedIndex) {
        for (int offset = from; offset + RECORD_HEADER + MIN_PAYLOAD <= file.length; offset++) {
            LogEntry e = readRecord(ByteBuffer.wrap(file, offset, file.length - offset));
            if (e != null && e.index() >= expectedIndex) return true;
        }
        return false;
    }

    // =====================================================================================
    // Hard state
    // =====================================================================================

    @Override public long currentTerm() { return term; }
    @Override public String votedFor() { return votedFor; }

    @Override
    public void saveHardState(long newTerm, String newVotedFor) {
        byte[] vote = newVotedFor == null ? new byte[0] : newVotedFor.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 4 + vote.length + 4);
        buf.putInt(HARD_STATE_MAGIC).putLong(newTerm).putInt(newVotedFor == null ? -1 : vote.length).put(vote);
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc.getValue()).flip();
        writeAtomically(dir.resolve("hardstate"), buf);
        term = newTerm;
        votedFor = newVotedFor;
    }

    /** Write-to-temp, fsync, rename, fsync directory: readers see the old or the new file, never a mix. */
    private void writeAtomically(Path target, ByteBuffer... content) {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                for (ByteBuffer b : content) {
                    while (b.hasRemaining()) ch.write(b);
                }
                if (syncMode == SyncMode.ALWAYS) ch.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            syncDirectory(target.getParent());
        } catch (IOException e) {
            throw new StorageException("failed to write " + target, e);
        }
    }

    private void syncDirectory(Path directory) throws IOException {
        if (syncMode != SyncMode.ALWAYS) return;
        try (FileChannel ch = FileChannel.open(directory, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            // Some platforms (Windows) cannot fsync a directory; durability of the rename then
            // relies on the filesystem's own ordering guarantees.
            if (!System.getProperty("os.name", "").toLowerCase().contains("win")) throw e;
        }
    }

    // =====================================================================================
    // Log
    // =====================================================================================

    @Override public long snapshotIndex() { return snapshotIndex; }
    @Override public long snapshotTerm() { return snapshotTerm; }
    @Override public long firstIndex() { return firstIndex; }
    @Override public long lastIndex() { return firstIndex + entries.size() - 1; }

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
        return entries.get((int) (index - firstIndex));
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndexInclusive) {
        if (fromIndex > toIndexInclusive) return List.of();
        entry(fromIndex);
        entry(toIndexInclusive);
        return List.copyOf(entries.subList((int) (fromIndex - firstIndex), (int) (toIndexInclusive - firstIndex + 1)));
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) return;
        ensureOpen();
        checkWriteFailure();
        long expected = lastIndex() + 1;
        for (LogEntry e : newEntries) {
            if (e.index() != expected++) throw new IllegalArgumentException("non-contiguous append at " + e.index());
        }
        List<LogEntry> batch = List.copyOf(newEntries);
        entries.addAll(batch);
        writer.execute(() -> writeRecords(batch));
    }

    @Override
    public long durableIndex() {
        checkWriteFailure();
        return Math.max(durableIndex, snapshotIndex);
    }

    @Override
    public void setDurabilityListener(Runnable listener) {
        durabilityListener = listener == null ? () -> { } : listener;
    }

    /** Writer thread: appends the records; fsyncs if nothing else is queued behind them. */
    private void writeRecords(List<LogEntry> newEntries) {
        if (writeFailure != null) return; // the log on disk ends before this batch; never acknowledge it
        try {
            int total = 0;
            for (LogEntry e : newEntries) total += RECORD_HEADER + MIN_PAYLOAD + e.data().length;
            Segment segment = activeSegmentFor(newEntries.get(0).index());
            ByteBuffer buf = ByteBuffer.allocate(total);
            List<Long> newOffsets = new ArrayList<>(newEntries.size());
            for (LogEntry e : newEntries) {
                newOffsets.add(segment.size + buf.position());
                int payload = MIN_PAYLOAD + e.data().length;
                buf.putInt(payload);
                int crcAt = buf.position();
                buf.putInt(0);
                int payloadAt = buf.position();
                buf.putLong(e.index()).putLong(e.term()).put((byte) e.type().ordinal()).put(e.data());
                CRC32C crc = new CRC32C();
                crc.update(buf.array(), payloadAt, payload);
                buf.putInt(crcAt, (int) crc.getValue());
            }
            buf.flip();
            while (buf.hasRemaining()) segment.channel.write(buf);
            segment.size += total;
            segment.nextIndex = newEntries.get(newEntries.size() - 1).index() + 1;
            offsets.addAll(newOffsets);
            writtenIndex = segment.nextIndex - 1;
            if (writer.getQueue().isEmpty()) syncWritten();
        } catch (IOException | RuntimeException e) {
            writeFailure = new StorageException("WAL append failed", e);
            notifyListener();
        }
    }

    /** Writer thread: makes everything written so far durable and publishes the new durable index. */
    private void syncWritten() throws IOException {
        if (writtenIndex == durableIndex) return;
        if (syncMode == SyncMode.ALWAYS && !segments.isEmpty()) segments.get(segments.size() - 1).channel.force(false);
        durableIndex = writtenIndex;
        notifyListener();
    }

    private void notifyListener() {
        try {
            durabilityListener.run();
        } catch (RuntimeException ignored) {
            // the listener's executor may already be shut down; the Raft thread will see the state when it next looks
        }
    }

    /** Waits until every queued write is durable. Afterwards the Raft thread may touch segments and offsets. */
    private void drain() {
        try {
            writer.submit(() -> {
                if (writeFailure == null) syncWritten();
                return null;
            }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("interrupted while waiting for the WAL", e);
        } catch (ExecutionException e) {
            writeFailure = new StorageException("WAL sync failed", e.getCause());
        }
        checkWriteFailure();
    }

    private void checkWriteFailure() {
        StorageException failure = writeFailure;
        if (failure != null) throw new StorageException(failure.getMessage(), failure);
    }

    /** The segment that the entry at {@code index} should be appended to, rolling if necessary. */
    private Segment activeSegmentFor(long index) throws IOException {
        Segment last = segments.isEmpty() ? null : segments.get(segments.size() - 1);
        if (last != null && last.nextIndex == index && last.size < segmentBytes) return last;
        if (last != null && syncMode == SyncMode.ALWAYS) last.channel.force(true);
        Path path = walDir.resolve(String.format("%020d.wal", index));
        Segment segment = new Segment(path, index);
        segment.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        ByteBuffer header = ByteBuffer.allocate(SEGMENT_HEADER);
        header.put(WAL_MAGIC).putLong(index).flip();
        while (header.hasRemaining()) segment.channel.write(header);
        if (syncMode == SyncMode.ALWAYS) segment.channel.force(true);
        syncDirectory(walDir);
        segment.size = SEGMENT_HEADER;
        segments.add(segment);
        return segment;
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        ensureOpen();
        if (fromIndex <= snapshotIndex) throw new IllegalArgumentException("cannot truncate into the snapshot");
        if (fromIndex > lastIndex()) return;
        drain();
        try {
            // Newest segments first so a crash part-way leaves a contiguous (shorter) log.
            while (!segments.isEmpty() && segments.get(segments.size() - 1).firstIndex >= fromIndex) {
                deleteSegment(segments.remove(segments.size() - 1));
            }
            int position = (int) (fromIndex - firstIndex);
            if (!segments.isEmpty()) {
                Segment segment = segments.get(segments.size() - 1);
                if (segment.nextIndex > fromIndex) {
                    long offset = offsets.get(position);
                    segment.channel.truncate(offset);
                    segment.channel.position(offset);
                    if (syncMode == SyncMode.ALWAYS) segment.channel.force(true);
                    segment.size = offset;
                    segment.nextIndex = fromIndex;
                }
            }
            syncDirectory(walDir);
            entries.subList(position, entries.size()).clear();
            offsets.subList(position, offsets.size()).clear();
            writtenIndex = fromIndex - 1;
            durableIndex = writtenIndex;
        } catch (IOException e) {
            throw new StorageException("WAL truncate failed", e);
        }
    }

    private void deleteSegment(Segment segment) throws IOException {
        segment.channel.close();
        Files.deleteIfExists(segment.path);
    }

    // =====================================================================================
    // Snapshots and compaction
    // =====================================================================================

    @Override
    public byte[] loadSnapshot() {
        if (snapshotPath == null) return null;
        try {
            byte[] file = Files.readAllBytes(snapshotPath);
            return java.util.Arrays.copyOfRange(file, 28, file.length);
        } catch (IOException e) {
            throw new StorageException("cannot read snapshot " + snapshotPath, e);
        }
    }

    @Override
    public void saveSnapshot(long index, long snapTerm, byte[] data, long trailingEntries) {
        ensureOpen();
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("snapshot index " + index + " not in log");
        }
        drain();
        writeSnapshotFile(index, snapTerm, data);
        compactThrough(index, Math.max(firstIndex, index + 1 - trailingEntries));
    }

    @Override
    public void installSnapshot(long index, long snapTerm, byte[] data) {
        ensureOpen();
        boolean keepSuffix = index > snapshotIndex && index <= lastIndex() && termAt(index) == snapTerm;
        drain();
        // Order matters for crash safety: the snapshot becomes durable first, only then is log
        // removed. A crash in between leaves snapshot + old log, which recovery handles.
        writeSnapshotFile(index, snapTerm, data);
        compactThrough(index, keepSuffix ? index + 1 : -1);
    }

    private void writeSnapshotFile(long index, long snapTerm, byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data, 0, data.length);
        ByteBuffer header = ByteBuffer.allocate(28);
        header.putInt(SNAPSHOT_MAGIC).putLong(index).putLong(snapTerm).putInt(data.length).putInt((int) crc.getValue()).flip();
        Path target = snapshotDir.resolve(String.format("%020d-%d.snap", index, snapTerm));
        writeAtomically(target, header, ByteBuffer.wrap(data));
        Path previous = snapshotPath;
        snapshotPath = target;
        try {
            if (previous != null && !previous.equals(target)) Files.deleteIfExists(previous);
        } catch (IOException e) {
            throw new StorageException("cannot remove old snapshot " + previous, e);
        }
    }

    /**
     * Records the snapshot at {@code index}, drops in-memory entries before {@code keepFrom} and deletes
     * segments that are no longer needed. {@code keepFrom < 0} discards the whole log.
     */
    private void compactThrough(long index, long keepFrom) {
        try {
            if (keepFrom >= 0) {
                int drop = (int) (keepFrom - firstIndex);
                entries.subList(0, drop).clear();
                offsets.subList(0, drop).clear();
                firstIndex = keepFrom;
                // A segment is obsolete once the next segment starts at or before keepFrom.
                while (segments.size() > 1 && segments.get(1).firstIndex <= keepFrom) {
                    deleteSegment(segments.remove(0));
                }
            } else {
                entries.clear();
                offsets.clear();
                firstIndex = index + 1;
                while (!segments.isEmpty()) {
                    deleteSegment(segments.remove(segments.size() - 1));
                }
                writtenIndex = index;
                durableIndex = index;
            }
            syncDirectory(walDir);
        } catch (IOException e) {
            throw new StorageException("log compaction failed", e);
        }
        snapshotIndex = index;
        snapshotTerm = snapshotPath == null ? 0 : parseTerm(snapshotPath);
    }

    private static long parseTerm(Path snapshot) {
        String name = snapshot.getFileName().toString();
        return Long.parseLong(name.substring(name.indexOf('-') + 1, name.length() - ".snap".length()));
    }

    // =====================================================================================

    @Override
    public long diskUsageBytes() {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0;
                }
            }).sum();
        } catch (IOException e) {
            return 0;
        }
    }

    private void ensureOpen() {
        if (closed) throw new StorageException("storage is closed");
    }

    @Override
    public void close() {
        if (closed) return;
        try {
            drain();
        } catch (StorageException e) {
            // a failed write was never acknowledged; still release the files below
        }
        closed = true;
        writer.shutdownNow();
        try {
            for (Segment s : segments) s.channel.close();
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            throw new StorageException("error closing storage", e);
        }
    }
}
