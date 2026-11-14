package io.quorumdb.faultlab.scenario;

import io.quorumdb.faultlab.check.LinearizabilityChecker.Kind;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Op;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Outcome;
import io.quorumdb.faultlab.sim.SimCluster;
import io.quorumdb.faultlab.sim.SimCluster.SimNode;
import io.quorumdb.kv.KvCommand;
import io.quorumdb.kv.KvStateMachine;
import io.quorumdb.raft.Outcomes;
import io.quorumdb.raft.RaftEvent;

import java.util.List;

/**
 * A simulated client. It issues one operation at a time, follows leader hints, retries on
 * definite failures and on unknown outcomes (safe because writes carry a client id and sequence
 * number that the state machine de-duplicates), and gives up at a deadline.
 *
 * <p>What it records is exactly what a real client could know: when it invoked the operation,
 * when it got an answer, and what the answer was.
 */
final class SimClient {
    private static final long CLIENT_LATENCY_MS = 1;
    private static final long ATTEMPT_TIMEOUT_MS = 400;
    private static final long RETRY_BACKOFF_MS = 15;

    private final String id;
    private final SimCluster cluster;
    private final Scenario.Workload workload;
    private final List<Op> history;
    private final long stopAtMs;
    private final List<String> nodeIds;

    private long sequence;
    private String knownLeader;
    private int rotation;

    /** State of the operation in progress. */
    private final class Call {
        final long opId;
        final Kind kind;
        final String key;
        final String value;
        final long invokedAt;
        final long deadline;
        final long seq;
        boolean settled;
        boolean mayHaveExecuted;
        int attempt;

        Call(long opId, Kind kind, String key, String value, long seq) {
            this.opId = opId;
            this.kind = kind;
            this.key = key;
            this.value = value;
            this.seq = seq;
            this.invokedAt = cluster.loop().nowMs();
            this.deadline = invokedAt + workload.opTimeoutMs();
        }
    }

    /** Operation id counter shared by all clients of one run (single-element array as a mutable cell). */
    private final long[] opIds;

    SimClient(String id, SimCluster cluster, Scenario.Workload workload, List<Op> history, long stopAtMs, long[] opIds) {
        this.opIds = opIds;
        this.id = id;
        this.cluster = cluster;
        this.workload = workload;
        this.history = history;
        this.stopAtMs = stopAtMs;
        this.nodeIds = cluster.nodeIds();
        this.rotation = cluster.random().nextInt(nodeIds.size());
    }

    void start() {
        scheduleNext();
    }

    private void scheduleNext() {
        long think = workload.opIntervalMs() / 2 + cluster.random().nextInt(workload.opIntervalMs().intValue() + 1);
        cluster.loop().schedule(think, this::begin);
    }

    private void begin() {
        if (cluster.loop().nowMs() >= stopAtMs) return;
        double roll = cluster.random().nextDouble();
        String key = "k" + cluster.random().nextInt(workload.keys());
        Call call;
        long opId = ++opIds[0];
        if (roll < workload.readRatio()) {
            call = new Call(opId, Kind.GET, key, null, 0);
        } else if (roll < workload.readRatio() + workload.deleteRatio()) {
            call = new Call(opId, Kind.DELETE, key, null, ++sequence);
        } else {
            call = new Call(opId, Kind.PUT, key, id + "-" + opId, ++sequence); // values are unique per operation
        }
        cluster.emitFrom(id, RaftEvent.Type.CLIENT_REQUEST, null, opId,
                call.kind + " " + key + (call.value == null ? "" : "=" + call.value));
        cluster.loop().schedule(workload.opTimeoutMs(), () -> onDeadline(call));
        attempt(call);
    }

    private void attempt(Call call) {
        if (call.settled) return;
        int attemptNo = ++call.attempt;
        String target = knownLeader != null ? knownLeader : nodeIds.get(rotation++ % nodeIds.size());
        cluster.loop().schedule(CLIENT_LATENCY_MS, () -> arrive(call, attemptNo, target));
        // If this attempt gets no answer at all (node crashed or cannot commit), try another node.
        cluster.loop().schedule(ATTEMPT_TIMEOUT_MS, () -> {
            if (!call.settled && call.attempt == attemptNo) {
                knownLeader = null;
                attempt(call);
            }
        });
    }

    private void arrive(Call call, int attemptNo, String target) {
        if (call.settled) return;
        SimNode node = cluster.node(target);
        if (!node.isServing()) { // connection refused: definitely not executed
            retry(call, attemptNo, null);
            return;
        }
        if (call.kind == Kind.GET) {
            read(call, attemptNo, node);
            return;
        }
        KvCommand.Op op = call.kind == Kind.PUT ? KvCommand.Op.put(call.key, call.value) : KvCommand.Op.delete(call.key);
        byte[] command = KvCommand.single(id, call.seq, op).encode();
        call.mayHaveExecuted |= node.raft().isLeader(); // from here on the command may be in a log
        node.raft().propose(command, outcome -> respond(() -> {
            switch (outcome.status()) {
                case OK -> settle(call, Outcome.OK, null, "index=" + outcome.index());
                case NOT_LEADER, BUSY -> retry(call, attemptNo, outcome.leaderHint());
                case UNKNOWN -> {
                    call.mayHaveExecuted = true;
                    retry(call, attemptNo, null);
                }
            }
        }));
    }

    private void read(Call call, int attemptNo, SimNode node) {
        switch (workload.readMode()) {
            case LINEARIZABLE -> node.raft().linearizableRead(outcome -> {
                if (outcome.status() == Outcomes.Status.OK) {
                    // Read the state machine now, on the node thread, inside the callback.
                    String value = valueOf(node.kv().get(call.key));
                    respond(() -> settle(call, Outcome.OK, value, "readIndex=" + outcome.readIndex()));
                } else {
                    respond(() -> retry(call, attemptNo, outcome.leaderHint()));
                }
            });
            case LEADER_LOCAL -> {
                if (node.raft().isLeader()) {
                    String value = valueOf(node.kv().get(call.key));
                    respond(() -> settle(call, Outcome.OK, value, "leader-local from " + node.id));
                } else {
                    String hint = node.raft().leaderId();
                    respond(() -> retry(call, attemptNo, hint));
                }
            }
            case STALE -> {
                String value = valueOf(node.kv().get(call.key));
                respond(() -> settle(call, Outcome.OK, value, "stale from " + node.id));
            }
        }
    }

    private static String valueOf(KvStateMachine.Versioned v) {
        return v == null ? null : v.value();
    }

    /** The reply travels back to the client. */
    private void respond(Runnable handler) {
        cluster.loop().schedule(CLIENT_LATENCY_MS, handler);
    }

    private void retry(Call call, int attemptNo, String leaderHint) {
        if (call.settled || call.attempt != attemptNo) return;
        knownLeader = leaderHint;
        cluster.loop().schedule(RETRY_BACKOFF_MS, () -> {
            if (!call.settled && call.attempt == attemptNo) attempt(call);
        });
    }

    private void onDeadline(Call call) {
        if (call.settled) return;
        boolean unknown = call.kind != Kind.GET && call.mayHaveExecuted;
        settle(call, unknown ? Outcome.INDETERMINATE : Outcome.FAILED, null,
                unknown ? "timed out; the write may or may not have been committed" : "timed out; never executed");
    }

    private void settle(Call call, Outcome outcome, String readValue, String note) {
        if (call.settled) return;
        call.settled = true;
        long now = cluster.loop().nowMs();
        String value = call.kind == Kind.GET ? readValue : call.value;
        // An indeterminate operation never "completes" as far as real-time ordering is concerned.
        long completedAt = outcome == Outcome.INDETERMINATE ? Long.MAX_VALUE : now;
        history.add(new Op(call.opId, id, call.kind, call.key, value, call.invokedAt, completedAt, outcome));
        String shown = call.kind == Kind.GET && outcome == Outcome.OK ? " -> " + (readValue == null ? "(absent)" : readValue) : "";
        cluster.emitFrom(id, RaftEvent.Type.CLIENT_RESPONSE, null, call.opId,
                outcome + " " + call.kind + " " + call.key + shown + " after " + (now - call.invokedAt) + " ms; " + note);
        scheduleNext();
    }
}
