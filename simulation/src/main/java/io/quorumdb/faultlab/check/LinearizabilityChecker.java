package io.quorumdb.faultlab.check;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Black-box linearizability checker for the client-visible history of a key-value store.
 *
 * <p>It knows nothing about Raft: it only sees what clients saw (operation, invocation time,
 * completion time, result) and searches for a sequential order of the operations that respects
 * real time and register semantics. The search is the Wing &amp; Gong algorithm with Lowe's
 * memoisation of (linearized-set, state) configurations, the same family of algorithm used by
 * Knossos (Jepsen) and Porcupine. Keys are independent registers, so each key is checked on its
 * own (linearizability is compositional).
 *
 * <p>Operations whose outcome the client never learned (timeouts, lost leadership) are
 * <em>indeterminate</em>: they may have taken effect at any time after invocation, or never.
 */
public final class LinearizabilityChecker {

    public enum Kind { PUT, DELETE, GET }

    public enum Outcome {
        /** The client received a definite answer. */
        OK,
        /** The client knows the operation did not execute; it is excluded from the search. */
        FAILED,
        /** The client does not know whether the operation executed. */
        INDETERMINATE
    }

    /**
     * One client operation.
     *
     * @param value for PUT: the value written; for GET with {@link Outcome#OK}: the value read (null = absent)
     */
    public record Op(long id, String client, Kind kind, String key, String value, long invokeMs,
                     long completeMs, Outcome outcome) {}

    public enum Result { LINEARIZABLE, NOT_LINEARIZABLE, UNDETERMINED }

    /**
     * @param problemOps for a non-linearizable key: the operations of that key (the evidence)
     */
    public record KeyVerdict(String key, Result result, int operations, String explanation, List<Long> problemOps) {}

    public record Report(Result result, int operationsChecked, int keysChecked, List<KeyVerdict> keys) {}

    private static final long STEP_BUDGET_PER_KEY = 2_000_000;

    public Report check(List<Op> history) {
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        for (Op op : history) {
            if (op.outcome() != Outcome.FAILED) byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        List<KeyVerdict> verdicts = new ArrayList<>();
        Result overall = Result.LINEARIZABLE;
        int checked = 0;
        for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
            KeyVerdict v = checkKey(e.getKey(), e.getValue());
            checked += v.operations();
            if (v.result() == Result.NOT_LINEARIZABLE) {
                overall = Result.NOT_LINEARIZABLE;
            } else if (v.result() == Result.UNDETERMINED && overall == Result.LINEARIZABLE) {
                overall = Result.UNDETERMINED;
            }
            if (v.result() != Result.LINEARIZABLE) verdicts.add(v);
        }
        return new Report(overall, checked, byKey.size(), verdicts);
    }

    private KeyVerdict checkKey(String key, List<Op> all) {
        // Reduce the search space without changing the answer:
        //  * an indeterminate GET has no effect and returned nothing: drop it;
        //  * an indeterminate PUT whose (unique) value nobody ever read can be assumed not to have
        //    happened: removing a write that no read observed keeps any valid order valid.
        Set<String> valuesRead = new HashSet<>();
        Map<String, Integer> writesOfValue = new LinkedHashMap<>();
        for (Op op : all) {
            if (op.kind() == Kind.GET && op.outcome() == Outcome.OK && op.value() != null) valuesRead.add(op.value());
            if (op.kind() == Kind.PUT) writesOfValue.merge(op.value(), 1, Integer::sum);
        }
        List<Op> ops = new ArrayList<>();
        for (Op op : all) {
            if (op.outcome() == Outcome.INDETERMINATE) {
                if (op.kind() == Kind.GET) continue;
                if (op.kind() == Kind.PUT && !valuesRead.contains(op.value()) && writesOfValue.get(op.value()) == 1) continue;
            }
            ops.add(op);
        }
        ops.sort((a, b) -> a.invokeMs() != b.invokeMs() ? Long.compare(a.invokeMs(), b.invokeMs()) : Long.compare(a.id(), b.id()));

        for (Op op : ops) {
            if (op.kind() == Kind.GET && op.value() != null && !writesOfValue.containsKey(op.value())) {
                return new KeyVerdict(key, Result.NOT_LINEARIZABLE, ops.size(),
                        "read op " + op.id() + " returned value '" + op.value() + "' that no client ever wrote",
                        List.of(op.id()));
            }
        }

        Search search = new Search(ops);
        Boolean ok = search.run();
        if (ok == null) {
            return new KeyVerdict(key, Result.UNDETERMINED, ops.size(),
                    "search budget exhausted after " + STEP_BUDGET_PER_KEY + " steps", List.of());
        }
        if (ok) return new KeyVerdict(key, Result.LINEARIZABLE, ops.size(), null, List.of());
        Op stuck = ops.get(search.deepestBlocked);
        return new KeyVerdict(key, Result.NOT_LINEARIZABLE, ops.size(),
                "no sequential order of the " + ops.size() + " operations on this key is consistent with real time; "
                        + "the search could never place " + stuck.kind() + " op " + stuck.id() + " by " + stuck.client()
                        + (stuck.kind() == Kind.GET ? " (returned " + (stuck.value() == null ? "absent" : "'" + stuck.value() + "'") + ")" : ""),
                ops.stream().map(Op::id).toList());
    }

    /** Depth-first search over linearization orders with memoisation. */
    private static final class Search {
        private record Config(BitSet done, String state) {}

        private final List<Op> ops;
        private final int n;
        private final BitSet done;
        private final Set<Config> failed = new HashSet<>();
        private long steps;
        int deepestBlocked;
        private int deepestDepth = -1;

        Search(List<Op> ops) {
            this.ops = ops;
            this.n = ops.size();
            this.done = new BitSet(n);
        }

        Boolean run() {
            return dfs(null, 0);
        }

        private Boolean dfs(String state, int depth) {
            // Success once every operation with a definite result has been placed.
            boolean allDefinitePlaced = true;
            long horizon = Long.MAX_VALUE; // earliest completion among unplaced definite ops
            for (int i = 0; i < n; i++) {
                if (done.get(i)) continue;
                Op op = ops.get(i);
                if (op.outcome() == Outcome.OK) {
                    allDefinitePlaced = false;
                    horizon = Math.min(horizon, op.completeMs());
                }
            }
            if (allDefinitePlaced) return true;
            if (++steps > STEP_BUDGET_PER_KEY) return null;
            Config config = new Config((BitSet) done.clone(), state);
            if (failed.contains(config)) return false;

            int firstBlocked = -1;
            for (int i = 0; i < n; i++) {
                if (done.get(i)) continue;
                Op op = ops.get(i);
                // An op can be next only if it was invoked before every unplaced op completed.
                if (op.invokeMs() > horizon) break; // ops are sorted by invocation time
                String next;
                switch (op.kind()) {
                    case PUT -> next = op.value();
                    case DELETE -> next = null;
                    default -> {
                        if (op.outcome() == Outcome.OK && !Objects.equals(op.value(), state)) {
                            if (firstBlocked < 0) firstBlocked = i;
                            continue;
                        }
                        next = state;
                    }
                }
                done.set(i);
                Boolean result = dfs(next, depth + 1);
                done.clear(i);
                if (result == null || result) return result;
            }
            if (depth > deepestDepth && firstBlocked >= 0) {
                deepestDepth = depth;
                deepestBlocked = firstBlocked;
            }
            failed.add(config);
            return false;
        }
    }
}
