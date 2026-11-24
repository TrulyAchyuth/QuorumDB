package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.quorumdb.storage.StorageContractTest.entries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Crash recovery, torn writes and corruption handling of the file-backed storage. */
class FileStorageRecoveryTest {
    @TempDir
    Path dir;

    private FileStorage open() {
        return new FileStorage(dir, FileStorage.SyncMode.ALWAYS, 512);
    }

    /** Appends one entry per call, as a node does, so the log rolls across segments. */
    private static void appendEach(FileStorage s, long from, long to, long term) {
        for (LogEntry e : entries(from, to, term)) s.append(List.of(e));
    }

    private List<Path> segments() throws IOException {
        try (Stream<Path> s = Files.list(dir.resolve("wal"))) {
            return s.sorted().toList();
        }
    }

    @Test
    void snapshotTailSurvivesReopenIfItMatchesTheSnapshot() {
        try (FileStorage s = open()) {
            appendEach(s, 1, 40, 1);
            s.saveSnapshot(35, 1, "snap".getBytes(), 10);
            assertThat(s.firstIndex()).isEqualTo(26);
        }
        try (FileStorage s = open()) {
            assertThat(s.snapshotIndex()).isEqualTo(35);
            assertThat(s.firstIndex()).as("at least the configured tail is back (whole segments are kept)")
                    .isLessThanOrEqualTo(26);
            assertThat(s.termAt(30)).isEqualTo(1);
            assertThat(s.lastIndex()).isEqualTo(40);
        }
    }

    @Test
    void entriesBeforeTheSnapshotAreDroppedOnReopenIfTheyDoNotMatchIt() throws IOException {
        try (FileStorage s = open()) {
            appendEach(s, 1, 20, 1);
        }
        Path backup = dir.resolveSibling(dir.getFileName() + "-wal-backup");
        Files.createDirectories(backup);
        for (Path segment : segments()) Files.copy(segment, backup.resolve(segment.getFileName()));
        try (FileStorage s = open()) {
            s.installSnapshot(15, 2, "snap".getBytes()); // a leader's snapshot; our entry 15 (term 1) does not match
        }
        // Crash after the snapshot became durable but before the old log was removed.
        try (Stream<Path> files = Files.list(backup)) {
            for (Path segment : files.toList()) Files.copy(segment, dir.resolve("wal").resolve(segment.getFileName()));
        }
        try (FileStorage s = open()) {
            assertThat(s.snapshotIndex()).isEqualTo(15);
            assertThat(s.firstIndex()).as("old entries 1..15 must not pass for the snapshot's history").isEqualTo(16);
            assertThatThrownBy(() -> s.entry(15)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void everythingSurvivesReopen() {
        try (FileStorage s = open()) {
            s.saveHardState(7, "n3");
            s.append(entries(1, 40, 2));
            s.truncateSuffix(31);
            s.append(entries(31, 35, 7));
            s.saveSnapshot(20, 2, "snap".getBytes());
        }
        try (FileStorage s = open()) {
            assertThat(s.currentTerm()).isEqualTo(7);
            assertThat(s.votedFor()).isEqualTo("n3");
            assertThat(s.snapshotIndex()).isEqualTo(20);
            assertThat(s.snapshotTerm()).isEqualTo(2);
            assertThat(new String(s.loadSnapshot())).isEqualTo("snap");
            assertThat(s.lastIndex()).isEqualTo(35);
            assertThat(s.termAt(30)).isEqualTo(2);
            assertThat(s.termAt(31)).isEqualTo(7);
            assertThat(new String(s.entry(33).data())).isEqualTo("cmd-33");
            s.append(entries(36, 36, 7));
        }
        try (FileStorage s = open()) {
            assertThat(s.lastIndex()).isEqualTo(36);
        }
    }

    @Test
    void logRollsIntoSegmentsAndCompactionDeletesOldOnes() throws IOException {
        try (FileStorage s = open()) {
            appendEach(s, 1, 100, 1);
            int before = segments().size();
            assertThat(before).as("small segment size forces several segments").isGreaterThan(3);
            s.saveSnapshot(90, 1, new byte[10]);
            assertThat(segments().size()).isLessThan(before);
            assertThat(s.diskUsageBytes()).isPositive();
        }
        try (FileStorage s = open()) {
            assertThat(s.snapshotIndex()).isEqualTo(90);
            assertThat(s.lastIndex()).isEqualTo(100);
        }
    }

    @Test
    void tornFinalRecordIsDiscarded() throws IOException {
        try (FileStorage s = open()) {
            s.append(entries(1, 10, 1));
        }
        Path last = segments().get(segments().size() - 1);
        // Simulate a crash in the middle of appending entry 11: a partial record at the end of the file.
        try (RandomAccessFile f = new RandomAccessFile(last.toFile(), "rw")) {
            f.seek(f.length());
            f.writeInt(40);          // length header says 40 bytes of payload follow...
            f.writeInt(0x1234);      // ...some checksum...
            f.write(new byte[9]);    // ...but only 9 bytes made it to disk.
        }
        try (FileStorage s = open()) {
            assertThat(s.lastIndex()).as("acknowledged entries intact, torn one dropped").isEqualTo(10);
            s.append(entries(11, 11, 1));
        }
        try (FileStorage s = open()) {
            assertThat(s.lastIndex()).isEqualTo(11);
        }
    }

    @Test
    void truncatedTailOfLastRecordIsTreatedAsTornWrite() throws IOException {
        try (FileStorage s = open()) {
            s.append(entries(1, 3, 1));
        }
        Path last = segments().get(segments().size() - 1);
        try (RandomAccessFile f = new RandomAccessFile(last.toFile(), "rw")) {
            f.setLength(f.length() - 3);
        }
        try (FileStorage s = open()) {
            assertThat(s.lastIndex()).isEqualTo(2);
        }
    }

    @Test
    void corruptionInTheMiddleOfTheLogIsFatal() throws IOException {
        try (FileStorage s = open()) {
            s.append(entries(1, 6, 1));
        }
        // Flip a byte inside an early record of the newest segment; valid records follow it.
        Path last = segments().get(segments().size() - 1);
        try (RandomAccessFile f = new RandomAccessFile(last.toFile(), "rw")) {
            f.seek(16 + 12);
            int b = f.read();
            f.seek(16 + 12);
            f.write(b ^ 0xff);
        }
        assertThatThrownBy(this::open).isInstanceOf(StorageException.Corrupted.class)
                .hasMessageContaining("followed by valid data");
    }

    @Test
    void corruptionInAnOlderSegmentIsFatal() throws IOException {
        try (FileStorage s = open()) {
            appendEach(s, 1, 100, 1);
        }
        Path first = segments().get(0);
        try (RandomAccessFile f = new RandomAccessFile(first.toFile(), "rw")) {
            f.setLength(f.length() - 5);
        }
        assertThatThrownBy(this::open).isInstanceOf(StorageException.Corrupted.class);
    }

    @Test
    void corruptHardStateIsFatal() throws IOException {
        try (FileStorage s = open()) {
            s.saveHardState(3, "n1");
        }
        byte[] bytes = Files.readAllBytes(dir.resolve("hardstate"));
        bytes[6] ^= 0x01;
        Files.write(dir.resolve("hardstate"), bytes);
        assertThatThrownBy(this::open).isInstanceOf(StorageException.Corrupted.class).hasMessageContaining("hardstate");
    }

    @Test
    void interruptedAtomicWritesLeaveOldStateVisible() throws IOException {
        try (FileStorage s = open()) {
            s.saveHardState(3, "n1");
            s.append(entries(1, 5, 3));
            s.saveSnapshot(4, 3, "good".getBytes());
        }
        // Crash after writing temp files but before the rename.
        Files.write(dir.resolve("hardstate.tmp"), new byte[] {1, 2, 3});
        Files.write(dir.resolve("snapshots").resolve("00000000000000000005-3.snap.tmp"), new byte[] {9, 9});
        try (FileStorage s = open()) {
            assertThat(s.currentTerm()).isEqualTo(3);
            assertThat(s.snapshotIndex()).isEqualTo(4);
            assertThat(new String(s.loadSnapshot())).isEqualTo("good");
        }
        assertThat(Files.exists(dir.resolve("hardstate.tmp"))).isFalse();
    }

    @Test
    void missingLogAfterSnapshotIsDetected() throws IOException {
        try (FileStorage s = open()) {
            appendEach(s, 1, 100, 1);
            s.saveSnapshot(50, 1, "snap".getBytes());
        }
        // Lose the snapshot: the compacted prefix can no longer be reconstructed.
        try (Stream<Path> snaps = Files.list(dir.resolve("snapshots"))) {
            for (Path p : snaps.toList()) Files.delete(p);
        }
        assertThatThrownBy(this::open).isInstanceOf(StorageException.Corrupted.class).hasMessageContaining("missing");
    }

    @Test
    void installSnapshotThatDiscardsLogSurvivesReopen() {
        try (FileStorage s = open()) {
            s.append(entries(1, 30, 1));
            s.installSnapshot(100, 5, "far-ahead".getBytes());
            assertThat(s.lastIndex()).isEqualTo(100);
            s.append(List.of(new LogEntry(101, 5, LogEntry.Type.NOOP, new byte[0])));
        }
        try (FileStorage s = open()) {
            assertThat(s.snapshotIndex()).isEqualTo(100);
            assertThat(s.lastIndex()).isEqualTo(101);
            assertThat(s.entry(101).type()).isEqualTo(LogEntry.Type.NOOP);
        }
    }

    @Test
    void dataDirectoryCannotBeOpenedTwice() {
        try (FileStorage s = open()) {
            assertThat(s.lastIndex()).isZero();
            assertThatThrownBy(this::open).isInstanceOf(StorageException.class).hasMessageContaining("already open");
        }
    }

    @Test
    void faultyStorageRejectsWritesButServesReads() {
        FaultyStorage s = new FaultyStorage(open());
        s.append(entries(1, 2, 1));
        s.setFailWrites(true);
        assertThatThrownBy(() -> s.append(entries(3, 3, 1))).isInstanceOf(StorageException.class);
        assertThatThrownBy(() -> s.saveHardState(2, null)).isInstanceOf(StorageException.class);
        assertThat(s.lastIndex()).isEqualTo(2);
        s.setFailWrites(false);
        s.append(entries(3, 3, 1));
        s.close();
    }
}
