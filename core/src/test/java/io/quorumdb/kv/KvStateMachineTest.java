package io.quorumdb.kv;

import io.quorumdb.raft.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KvStateMachineTest {
    private final KvStateMachine kv = new KvStateMachine();
    private long index;

    private KvResult apply(KvCommand command) {
        return KvResult.decode(kv.apply(new LogEntry(++index, 1, LogEntry.Type.COMMAND, command.encode())));
    }

    private KvResult put(String key, String value) {
        return apply(KvCommand.single("", 0, KvCommand.Op.put(key, value)));
    }

    @Test
    void putGetDeleteAndExistence() {
        assertThat(kv.contains("a")).isFalse();
        KvResult first = put("a", "1");
        assertThat(first.applied()).isTrue();
        assertThat(first.ops().get(0).existed()).isFalse();
        assertThat(kv.get("a")).isEqualTo(new KvStateMachine.Versioned("1", 1));

        assertThat(put("a", "2").ops().get(0).existed()).isTrue();
        assertThat(kv.get("a").version()).isEqualTo(2);

        KvResult deleted = apply(KvCommand.single("", 0, KvCommand.Op.delete("a")));
        assertThat(deleted.ops().get(0).existed()).isTrue();
        assertThat(kv.get("a")).isNull();
        assertThat(apply(KvCommand.single("", 0, KvCommand.Op.delete("a"))).ops().get(0).existed()).isFalse();
    }

    @Test
    void compareAndSetUsesVersions() {
        KvCommand createIfAbsent = KvCommand.single("", 0, new KvCommand.Op(KvCommand.OpType.PUT, "k", "v1", 0));
        assertThat(apply(createIfAbsent).applied()).isTrue();
        assertThat(apply(createIfAbsent).applied()).as("key now exists").isFalse();

        long version = kv.get("k").version();
        assertThat(apply(KvCommand.single("", 0, new KvCommand.Op(KvCommand.OpType.PUT, "k", "v2", version + 7))).applied()).isFalse();
        assertThat(kv.get("k").value()).isEqualTo("v1");
        assertThat(apply(KvCommand.single("", 0, new KvCommand.Op(KvCommand.OpType.PUT, "k", "v2", version))).applied()).isTrue();
        assertThat(kv.get("k").value()).isEqualTo("v2");
    }

    @Test
    void batchIsAllOrNothing() {
        put("x", "old");
        KvCommand batch = new KvCommand("", 0, List.of(
                KvCommand.Op.put("a", "1"),
                new KvCommand.Op(KvCommand.OpType.PUT, "x", "new", 999), // precondition fails
                KvCommand.Op.delete("x")));
        KvResult result = apply(batch);
        assertThat(result.applied()).isFalse();
        assertThat(kv.contains("a")).as("no partial application").isFalse();
        assertThat(kv.get("x").value()).isEqualTo("old");

        KvResult ok = apply(new KvCommand("", 0, List.of(KvCommand.Op.put("a", "1"), KvCommand.Op.put("b", "2"), KvCommand.Op.delete("x"))));
        assertThat(ok.applied()).isTrue();
        assertThat(kv.size()).isEqualTo(2);
        assertThat(kv.get("a").version()).isEqualTo(kv.get("b").version());
    }

    @Test
    void retriedCommandIsExecutedOnce() {
        KvCommand command = KvCommand.single("client-1", 5, new KvCommand.Op(KvCommand.OpType.PUT, "counter", "v", 0));
        KvResult first = apply(command);
        KvResult retry = apply(command);
        assertThat(first.applied()).isTrue();
        assertThat(first.duplicate()).isFalse();
        assertThat(retry.duplicate()).as("retry answered from the session cache").isTrue();
        assertThat(retry.applied()).as("cached result of the original execution").isTrue();
        assertThat(kv.get("counter").version()).as("not re-applied at the retry's index").isEqualTo(1);

        assertThat(apply(KvCommand.single("client-1", 6, KvCommand.Op.put("counter", "w"))).duplicate()).isFalse();
        assertThat(apply(KvCommand.single("client-2", 5, KvCommand.Op.put("other", "w"))).duplicate())
                .as("sequences are per client").isFalse();
    }

    @Test
    void snapshotRoundTripPreservesDataVersionsAndSessions() {
        put("a", "1");
        put("b", "2");
        KvCommand tracked = KvCommand.single("c", 1, KvCommand.Op.put("c", "3"));
        apply(tracked);
        byte[] snapshot = kv.snapshot();

        KvStateMachine restored = new KvStateMachine();
        restored.restore(snapshot);
        assertThat(restored.digest()).isEqualTo(kv.digest());
        assertThat(restored.get("b")).isEqualTo(kv.get("b"));
        assertThat(restored.lastAppliedIndex()).isEqualTo(3);
        // The session table travels with the snapshot, so de-duplication survives compaction.
        KvResult retry = KvResult.decode(restored.apply(new LogEntry(4, 1, LogEntry.Type.COMMAND, tracked.encode())));
        assertThat(retry.duplicate()).isTrue();
    }

    @Test
    void digestDependsOnContentNotHistory() {
        KvStateMachine other = new KvStateMachine();
        put("a", "1");
        other.apply(new LogEntry(1, 1, LogEntry.Type.COMMAND, KvCommand.single("", 0, KvCommand.Op.put("a", "1")).encode()));
        assertThat(other.digest()).isEqualTo(kv.digest());
        put("a", "2");
        assertThat(other.digest()).isNotEqualTo(kv.digest());
    }

    @Test
    void malformedCommittedEntryIsRejectedDeterministicallyWithoutChangingState() {
        put("a", "1");
        long digest = kv.digest();
        byte[] result = kv.apply(new LogEntry(++index, 1, LogEntry.Type.COMMAND, "not a command".getBytes()));
        KvResult decoded = KvResult.decode(result);
        assertThat(decoded.applied()).isFalse();
        assertThat(decoded.ops()).isEmpty();
        assertThat(kv.digest()).isEqualTo(digest);
        assertThat(kv.lastAppliedIndex()).as("the entry still counts as applied").isEqualTo(index);
        assertThat(put("b", "2").applied()).as("the state machine keeps working").isTrue();
    }

    @Test
    void scanReturnsOrderedPrefixMatches() {
        put("user:2", "b");
        put("user:1", "a");
        put("order:1", "x");
        assertThat(kv.scan("user:", 10)).extracting(e -> e.getKey()).containsExactly("user:1", "user:2");
        assertThat(kv.scan("", 2)).hasSize(2);
    }

    @Test
    void codecsRoundTripAndRejectGarbage() {
        KvCommand command = new KvCommand("client", 42, List.of(KvCommand.Op.put("k", "vé\n"), KvCommand.Op.delete("d"),
                new KvCommand.Op(KvCommand.OpType.PUT, "c", "x".repeat(100_000), 7)));
        assertThat(KvCommand.decode(command.encode())).isEqualTo(command);
        assertThatThrownBy(() -> KvCommand.decode(new byte[] {1, 2, 3})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KvCommand.decode(new byte[] {9})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KvStateMachine().restore(new byte[] {0, 0, 0, 0})).isInstanceOf(IllegalArgumentException.class);
    }
}
