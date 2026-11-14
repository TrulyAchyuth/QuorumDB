package io.quorumdb.storage;

import io.quorumdb.raft.LogEntry;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behaviour every {@link RaftStorage} must share, run against the in-memory and file implementations. */
class StorageContractTest {
    @TempDir
    Path dir;

    private RaftStorage open(String kind) {
        return kind.equals("memory") ? new MemoryStorage() : new FileStorage(dir, FileStorage.SyncMode.ALWAYS, 256);
    }

    static List<LogEntry> entries(long from, long to, long term) {
        List<LogEntry> out = new ArrayList<>();
        for (long i = from; i <= to; i++) out.add(new LogEntry(i, term, LogEntry.Type.COMMAND, ("cmd-" + i).getBytes()));
        return out;
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "file"})
    void startsEmpty(String kind) {
        try (RaftStorage s = open(kind)) {
            assertThat(s.currentTerm()).isZero();
            assertThat(s.votedFor()).isNull();
            assertThat(s.lastIndex()).isZero();
            assertThat(s.termAt(0)).isZero();
            assertThat(s.snapshotIndex()).isZero();
            assertThat(s.loadSnapshot()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "file"})
    void appendReadAndTruncate(String kind) {
        try (RaftStorage s = open(kind)) {
            s.append(entries(1, 5, 1));
            s.append(entries(6, 8, 2));
            assertThat(s.lastIndex()).isEqualTo(8);
            assertThat(s.termAt(5)).isEqualTo(1);
            assertThat(s.termAt(6)).isEqualTo(2);
            assertThat(s.entries(4, 7)).extracting(LogEntry::index).containsExactly(4L, 5L, 6L, 7L);
            assertThat(new String(s.entry(3).data())).isEqualTo("cmd-3");

            s.truncateSuffix(6);
            assertThat(s.lastIndex()).isEqualTo(5);
            s.append(entries(6, 6, 3));
            assertThat(s.termAt(6)).isEqualTo(3);

            assertThatThrownBy(() -> s.append(entries(9, 9, 3))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> s.entry(7)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "file"})
    void hardStateIsRemembered(String kind) {
        try (RaftStorage s = open(kind)) {
            s.saveHardState(3, "n2");
            assertThat(s.currentTerm()).isEqualTo(3);
            assertThat(s.votedFor()).isEqualTo("n2");
            s.saveHardState(4, null);
            assertThat(s.votedFor()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "file"})
    void snapshotCompactsThePrefix(String kind) {
        try (RaftStorage s = open(kind)) {
            s.append(entries(1, 10, 1));
            s.saveSnapshot(6, 1, "state@6".getBytes());
            assertThat(s.snapshotIndex()).isEqualTo(6);
            assertThat(s.snapshotTerm()).isEqualTo(1);
            assertThat(new String(s.loadSnapshot())).isEqualTo("state@6");
            assertThat(s.lastIndex()).isEqualTo(10);
            assertThat(s.termAt(6)).as("term at the snapshot boundary stays known").isEqualTo(1);
            assertThat(s.entries(7, 10)).hasSize(4);
            assertThatThrownBy(() -> s.entry(6)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> s.truncateSuffix(6)).isInstanceOf(IllegalArgumentException.class);

            s.saveSnapshot(10, 1, "state@10".getBytes());
            assertThat(s.lastIndex()).isEqualTo(10);
            s.append(entries(11, 12, 2));
            assertThat(s.lastIndex()).isEqualTo(12);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "file"})
    void installSnapshotKeepsMatchingSuffixOtherwiseDiscardsLog(String kind) {
        try (RaftStorage s = open(kind)) {
            s.append(entries(1, 10, 1));
            s.installSnapshot(5, 1, "leader@5".getBytes()); // we have a matching entry 5@1: keep 6..10
            assertThat(s.snapshotIndex()).isEqualTo(5);
            assertThat(s.lastIndex()).isEqualTo(10);

            s.installSnapshot(8, 4, "leader@8".getBytes()); // our entry 8 has term 1, not 4: log is stale
            assertThat(s.snapshotIndex()).isEqualTo(8);
            assertThat(s.snapshotTerm()).isEqualTo(4);
            assertThat(s.lastIndex()).isEqualTo(8);

            s.installSnapshot(20, 5, "leader@20".getBytes()); // far ahead of anything we have
            assertThat(s.lastIndex()).isEqualTo(20);
            s.append(entries(21, 21, 5));
            assertThat(s.termAt(20)).isEqualTo(5);
        }
    }
}
