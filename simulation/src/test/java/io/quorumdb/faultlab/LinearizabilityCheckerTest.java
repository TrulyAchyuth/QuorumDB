package io.quorumdb.faultlab;

import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Kind;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Op;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Outcome;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Result;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LinearizabilityCheckerTest {
    private final List<Op> history = new ArrayList<>();
    private long ids;

    private void put(String value, long invoke, long complete) {
        history.add(new Op(++ids, "c", Kind.PUT, "k", value, invoke, complete, Outcome.OK));
    }

    private void get(String value, long invoke, long complete) {
        history.add(new Op(++ids, "c", Kind.GET, "k", value, invoke, complete, Outcome.OK));
    }

    private void delete(long invoke, long complete) {
        history.add(new Op(++ids, "c", Kind.DELETE, "k", null, invoke, complete, Outcome.OK));
    }

    private void unknownPut(String value, long invoke) {
        history.add(new Op(++ids, "c", Kind.PUT, "k", value, invoke, Long.MAX_VALUE, Outcome.INDETERMINATE));
    }

    private Result result() {
        return new LinearizabilityChecker().check(history).result();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        get(null, 0, 1);
        put("a", 2, 3);
        get("a", 4, 5);
        put("b", 6, 7);
        get("b", 8, 9);
        delete(10, 11);
        get(null, 12, 13);
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }

    @Test
    void staleReadIsNotLinearizable() {
        put("a", 0, 1);
        put("b", 2, 3);
        get("a", 4, 5); // b was acknowledged before this read began
        LinearizabilityChecker.Report report = new LinearizabilityChecker().check(history);
        assertThat(report.result()).isEqualTo(Result.NOT_LINEARIZABLE);
        assertThat(report.keys()).singleElement().satisfies(k -> {
            assertThat(k.key()).isEqualTo("k");
            assertThat(k.explanation()).contains("GET");
        });
    }

    @Test
    void concurrentOperationsMayTakeEffectInEitherOrder() {
        put("a", 0, 10);
        put("b", 0, 10);
        get("a", 11, 12); // legal: b then a
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }

    @Test
    void readConcurrentWithWriteMaySeeOldOrNewValue() {
        put("a", 0, 1);
        put("b", 2, 10);
        get("a", 3, 4);
        get("b", 5, 6);
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }

    @Test
    void valueCannotFlipBackWithoutAWrite() {
        put("a", 0, 1);
        put("b", 2, 10);
        get("b", 3, 4);
        get("a", 5, 6); // once b was observed, a cannot reappear
        assertThat(result()).isEqualTo(Result.NOT_LINEARIZABLE);
    }

    @Test
    void readOfNeverWrittenValueIsRejected() {
        put("a", 0, 1);
        get("ghost", 2, 3);
        assertThat(result()).isEqualTo(Result.NOT_LINEARIZABLE);
    }

    @Test
    void indeterminateWriteMayTakeEffectLaterOrNever() {
        put("a", 0, 1);
        unknownPut("b", 2);
        get("a", 10, 11);  // b has not taken effect yet...
        get("b", 20, 21);  // ...and now it has: allowed, its outcome was unknown
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);

        history.clear();
        put("a", 0, 1);
        unknownPut("b", 2);
        get("a", 10, 11);  // never took effect: also allowed
        get("a", 20, 21);
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }

    @Test
    void indeterminateWriteCannotTakeEffectBeforeItWasInvoked() {
        put("a", 0, 1);
        get("b", 2, 3);
        unknownPut("b", 5);
        assertThat(result()).isEqualTo(Result.NOT_LINEARIZABLE);
    }

    @Test
    void definitelyFailedOperationsAreIgnored() {
        put("a", 0, 1);
        history.add(new Op(++ids, "c", Kind.PUT, "k", "never", 2, 3, Outcome.FAILED));
        get("a", 4, 5);
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }

    @Test
    void keysAreCheckedIndependently() {
        history.add(new Op(1, "c", Kind.PUT, "x", "1", 0, 1, Outcome.OK));
        history.add(new Op(2, "c", Kind.PUT, "y", "2", 0, 1, Outcome.OK));
        history.add(new Op(3, "c", Kind.GET, "x", "1", 2, 3, Outcome.OK));
        history.add(new Op(4, "c", Kind.GET, "y", null, 2, 3, Outcome.OK)); // y lost its write
        LinearizabilityChecker.Report report = new LinearizabilityChecker().check(history);
        assertThat(report.result()).isEqualTo(Result.NOT_LINEARIZABLE);
        assertThat(report.keys()).extracting(LinearizabilityChecker.KeyVerdict::key).containsExactly("y");
        assertThat(report.keysChecked()).isEqualTo(2);
    }

    @Test
    void handlesLongHistoriesWithManyClients() {
        // 2000 sequential-ish operations from interleaved clients; must finish quickly.
        long t = 0;
        String current = null;
        for (int i = 0; i < 2000; i++) {
            if (i % 3 == 0) {
                current = "v" + i;
                put(current, t, t + 2);
            } else {
                get(current, t, t + 2);
            }
            t += 3;
        }
        assertThat(result()).isEqualTo(Result.LINEARIZABLE);
    }
}
