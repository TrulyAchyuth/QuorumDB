package io.quorumdb.faultlab.scenario;

import io.quorumdb.faultlab.analysis.Explainer;
import io.quorumdb.faultlab.analysis.RunAnalyzer;
import io.quorumdb.faultlab.analysis.RunMetrics;
import io.quorumdb.faultlab.analysis.RunMetrics.Sample;
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.analysis.RunResult.FaultRecord;
import io.quorumdb.faultlab.analysis.RunResult.Headline;
import io.quorumdb.faultlab.analysis.RunResult.Liveness;
import io.quorumdb.faultlab.analysis.RunResult.NodeEnd;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Op;
import io.quorumdb.faultlab.check.NodeObservation;
import io.quorumdb.faultlab.check.SafetyChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario.FaultStep;
import io.quorumdb.faultlab.scenario.Scenario.FaultType;
import io.quorumdb.faultlab.scenario.Scenario.Trigger;
import io.quorumdb.faultlab.sim.SimCluster;
import io.quorumdb.faultlab.sim.SimCluster.SimNode;
import io.quorumdb.faultlab.sim.SimOptions;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.raft.RaftStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes a {@link Scenario} in the deterministic simulator and audits the result.
 *
 * <p>Phases: (1) run the cluster, workload and fault schedule for {@code durationMs} of virtual
 * time while the safety checker watches every event and samples every node's log; (2) stop the
 * workload and let the cluster settle so liveness can be judged; (3) check the client history
 * for linearizability, compute metrics and build the explanation.
 */
public final class ScenarioRunner {

    private final Scenario scenario;
    private final SimCluster cluster;
    private final SafetyChecker checker = new SafetyChecker(false);
    private final List<Op> history = new ArrayList<>();
    private final List<Sample> samples = new ArrayList<>();
    private final List<MutableFault> faults = new ArrayList<>();
    private final long settleMs;
    private final long maxEvents;
    private long eventCount;
    private boolean workloadRunning = true;

    private static final class MutableFault {
        int step;
        FaultType type;
        List<String> targets;
        String description;
        long injectedAt;
        long healedAt = -1;
        String leaderAtInjection;
        long termAtInjection;
        long commitAtInjection;
        boolean hitLeader;
        Runnable revert;
        boolean active;
    }

    /**
     * Thrown when a run's trace exceeds its event budget. The run is abandoned at that point, so a
     * scenario that would need gigabytes of memory fails fast instead of exhausting the heap.
     */
    public static final class RunTooLargeException extends RuntimeException {
        private final long maxEvents;
        private final long atMs;

        RunTooLargeException(long maxEvents, long atMs) {
            super("the run exceeded its budget of " + maxEvents + " events after " + atMs + " ms of simulated time");
            this.maxEvents = maxEvents;
            this.atMs = atMs;
        }

        public long maxEvents() { return maxEvents; }
        public long atMs() { return atMs; }
    }

    private ScenarioRunner(Scenario normalized, boolean recordMessages, long maxEvents) {
        if (maxEvents < 1) throw new IllegalArgumentException("maxEvents must be positive");
        this.maxEvents = maxEvents;
        this.scenario = normalized;
        Scenario.Cluster c = normalized.cluster();
        this.cluster = new SimCluster(new SimOptions(normalized.nodes(), normalized.seed(), c.electionTimeoutMinMs(),
                c.electionTimeoutMaxMs(), c.heartbeatIntervalMs(), c.preVote(), c.checkQuorum(), c.snapshotThreshold(),
                c.netLatencyMinMs(), c.netLatencyMaxMs(), recordMessages, 100));
        this.settleMs = 10 * c.electionTimeoutMaxMs() + 2 * normalized.workload().opTimeoutMs();
    }

    public static RunResult run(Scenario scenario) {
        return run(scenario, true);
    }

    /**
     * @param recordMessages include per-message events in the trace (bigger, more detailed timelines)
     */
    public static RunResult run(Scenario scenario, boolean recordMessages) {
        return run(scenario, recordMessages, Long.MAX_VALUE);
    }

    /**
     * @param maxEvents the most events the trace may hold; beyond it the run is abandoned
     * @throws RunTooLargeException if the trace grows past {@code maxEvents}
     */
    public static RunResult run(Scenario scenario, boolean recordMessages, long maxEvents) {
        return new ScenarioRunner(scenario.normalized(), recordMessages, maxEvents).execute();
    }

    private RunResult execute() {
        long[] opIds = new long[1];
        cluster.addListener(this::onEvent);
        cluster.startAll();

        Scenario.Workload w = scenario.workload();
        for (int i = 1; i <= w.clients(); i++) {
            new SimClient("client-" + i, cluster, w, history, scenario.durationMs(), opIds).start();
        }
        for (int i = 0; i < scenario.faults().size(); i++) {
            scheduleFault(i, scenario.faults().get(i));
        }
        scheduleSampling();

        cluster.loop().runUntil(scenario.durationMs());
        workloadRunning = false;
        // Settle: no new client operations; faults without an end stay in force (that is the scenario).
        cluster.loop().runUntil(scenario.durationMs() + settleMs);
        if (cluster.loop().halted()) throw new RunTooLargeException(maxEvents, cluster.loop().nowMs());
        observeAll();

        List<RaftEvent> events = List.copyOf(cluster.events());
        List<FaultRecord> faultRecords = faults.stream().map(f -> new FaultRecord(f.step, f.type, f.targets,
                f.description, f.injectedAt, f.healedAt, f.leaderAtInjection, f.termAtInjection, f.commitAtInjection,
                f.hitLeader)).toList();
        SafetyReport safety = checker.report();
        LinearizabilityChecker.Report linearizability = w.clients() == 0 ? null : new LinearizabilityChecker().check(history);
        long grace = 2 * scenario.cluster().electionTimeoutMaxMs() + w.opTimeoutMs();
        RunMetrics metrics = RunAnalyzer.analyze(events, faultRecords, history, samples, scenario.durationMs(),
                scenario.nodes(), grace,
                new RunAnalyzer.MessageCounts(cluster.messagesSent(), cluster.messagesDelivered(), cluster.messagesDropped()));
        Liveness liveness = judgeLiveness(metrics);
        Headline headline = RunAnalyzer.headline(safety, linearizability, liveness, metrics);
        return new RunResult(RunResult.Mode.SIMULATION, scenario, RunAnalyzer.traceHash(events), events, faultRecords,
                safety, linearizability, liveness, metrics, Explainer.explain(events, safety, linearizability, liveness),
                headline, finalState());
    }

    // =====================================================================================
    // Observation
    // =====================================================================================

    private void onEvent(RaftEvent e) {
        // Not thrown here: this runs inside Raft code, which treats exceptions as node failures. Stop the
        // event loop instead; execute() reports it once control is back outside the simulation.
        if (++eventCount > maxEvents) {
            cluster.loop().halt();
            return;
        }
        checker.onEvent(e);
        if (e.type() == RaftEvent.Type.LEADER_ELECTED) {
            // Look at the new leader's log right away: this is where Leader Completeness is decided.
            cluster.loop().schedule(0, this::observeAll);
        }
    }

    private void observeAll() {
        List<NodeObservation> round = new ArrayList<>();
        for (String id : cluster.nodeIds()) round.add(cluster.observe(id));
        checker.onObservations(round);
    }

    private void scheduleSampling() {
        long interval = cluster.options().observeIntervalMs();
        cluster.loop().schedule(interval, new Runnable() {
            @Override
            public void run() {
                observeAll();
                if (cluster.loop().nowMs() <= scenario.durationMs()) samples.add(sample());
                cluster.loop().schedule(interval, this);
            }
        });
    }

    private Sample sample() {
        SimNode leader = cluster.leader();
        long lag = 0, term = 0;
        int up = 0;
        for (SimNode n : cluster.nodes()) {
            if (n.isServing()) {
                up++;
                term = Math.max(term, n.raft().currentTerm());
            }
        }
        if (leader != null) {
            RaftStatus status = leader.raft().status();
            for (var peer : status.peers().entrySet()) {
                if (cluster.node(peer.getKey()).isServing()) {
                    lag = Math.max(lag, status.lastLogIndex() - peer.getValue().matchIndex());
                }
            }
        }
        long ok = history.stream().filter(o -> o.outcome() == LinearizabilityChecker.Outcome.OK).count();
        return new Sample(cluster.loop().nowMs(), cluster.maxCommitIndex(), term, leader == null ? null : leader.id, up,
                lag, ok, history.size() - ok);
    }

    // =====================================================================================
    // Faults
    // =====================================================================================

    private void scheduleFault(int step, FaultStep fault) {
        onTrigger(fault.start(), () -> inject(step, fault));
    }

    /** Runs {@code action} when the trigger fires: at a fixed time, or once its condition first holds. */
    private void onTrigger(Trigger trigger, Runnable action) {
        if (trigger.atMs() != null) {
            cluster.loop().schedule(Math.max(0, trigger.atMs() - cluster.loop().nowMs()), action);
            return;
        }
        long after = trigger.afterMs() == null ? 0 : trigger.afterMs();
        cluster.loop().schedule(5, new Runnable() {
            @Override
            public void run() {
                if (!workloadRunning) return; // conditions are not evaluated during the settle phase
                if (holds(trigger)) {
                    cluster.loop().schedule(after, action);
                } else {
                    cluster.loop().schedule(5, this);
                }
            }
        });
    }

    private boolean holds(Trigger trigger) {
        long value = trigger.value() == null ? 0 : trigger.value();
        return switch (trigger.when()) {
            case LEADER_ELECTED -> cluster.leader() != null;
            case COMMIT_INDEX_AT_LEAST -> cluster.maxCommitIndex() >= value;
            case OPS_COMPLETED_AT_LEAST -> history.stream()
                    .filter(o -> o.outcome() == LinearizabilityChecker.Outcome.OK).count() >= value;
        };
    }

    private List<String> resolve(List<String> targets) {
        List<String> out = new ArrayList<>();
        if (targets == null) return out;
        SimNode leader = cluster.leader();
        for (String t : targets) {
            String id = switch (t) {
                case "leader" -> leader != null ? leader.id : null;
                case "follower" -> {
                    List<String> followers = cluster.nodeIds().stream()
                            .filter(n -> (leader == null || !n.equals(leader.id)) && !out.contains(n)
                                    && cluster.node(n).isServing()).toList();
                    yield followers.isEmpty() ? null : followers.get(cluster.random().nextInt(followers.size()));
                }
                case "random" -> {
                    List<String> candidates = cluster.nodeIds().stream().filter(n -> !out.contains(n)).toList();
                    yield candidates.isEmpty() ? null : candidates.get(cluster.random().nextInt(candidates.size()));
                }
                default -> t;
            };
            if (id != null && !out.contains(id)) out.add(id);
        }
        return out;
    }

    private void inject(int step, FaultStep fault) {
        if (!workloadRunning && fault.start().atMs() == null) return;
        MutableFault record = new MutableFault();
        record.step = step;
        record.type = fault.type();
        record.injectedAt = cluster.loop().nowMs();
        SimNode leader = cluster.leader();
        record.leaderAtInjection = leader == null ? null : leader.id;
        record.termAtInjection = leader == null ? 0 : leader.raft().currentTerm();
        record.commitAtInjection = cluster.maxCommitIndex();
        List<String> targets = resolve(fault.targets());
        record.targets = targets;
        int quorum = scenario.nodes() / 2 + 1;

        switch (fault.type()) {
            case CRASH -> {
                record.description = "CRASH " + targets;
                record.hitLeader = leader != null && targets.contains(leader.id);
                announce(record);
                targets.forEach(cluster::crash);
                record.revert = () -> targets.forEach(cluster::restart);
            }
            case RESTART -> {
                record.description = "RESTART " + targets;
                record.hitLeader = leader != null && targets.contains(leader.id);
                announce(record);
                targets.forEach(cluster::restart);
            }
            case ISOLATE -> {
                record.description = "ISOLATE " + targets;
                record.hitLeader = leader != null && targets.contains(leader.id);
                announce(record);
                for (String t : targets) {
                    cluster.network().isolate(t, true);
                    cluster.emit(RaftEvent.Type.NODE_DISCONNECTED, t, "cut off from all peers");
                }
                record.revert = () -> {
                    for (String t : targets) {
                        cluster.network().isolate(t, false);
                        cluster.emit(RaftEvent.Type.NODE_RECOVERED, t, "reconnected");
                    }
                };
            }
            case PARTITION -> {
                List<List<String>> groups = new ArrayList<>();
                if (fault.groups() != null) {
                    for (List<String> g : fault.groups()) groups.add(resolve(g));
                } else {
                    groups.add(targets);
                    groups.add(cluster.nodeIds().stream().filter(n -> !targets.contains(n)).toList());
                }
                record.description = "PARTITION " + groups;
                record.targets = groups.stream().flatMap(List::stream).toList();
                if (leader != null) {
                    int leaderSide = groups.stream().filter(g -> g.contains(leader.id)).mapToInt(List::size).findFirst()
                            .orElse(scenario.nodes() - record.targets.size());
                    record.hitLeader = leaderSide < quorum;
                }
                announce(record);
                cluster.network().partition(groups);
                for (List<String> g : groups) {
                    if (g.size() < quorum) {
                        for (String t : g) cluster.emit(RaftEvent.Type.NODE_DISCONNECTED, t, "in minority group " + g);
                    }
                }
                record.revert = () -> {
                    cluster.network().healPartition();
                    for (String t : record.targets) cluster.emit(RaftEvent.Type.NODE_RECOVERED, t, "partition healed");
                };
            }
            case DELAY -> {
                record.description = "DELAY " + fault.delayMs() + " ms on " + targets;
                announce(record);
                targets.forEach(t -> cluster.network().setExtraDelay(t, fault.delayMs()));
                record.revert = () -> targets.forEach(t -> cluster.network().setExtraDelay(t, 0));
            }
            case DROP -> {
                record.description = "DROP " + Math.round(fault.dropProbability() * 100) + "% of messages on " + targets;
                announce(record);
                targets.forEach(t -> cluster.network().setDropProbability(t, fault.dropProbability()));
                record.revert = () -> targets.forEach(t -> cluster.network().setDropProbability(t, 0));
            }
            case STORAGE_FAILURE -> {
                record.description = "STORAGE_FAILURE on " + targets;
                record.hitLeader = leader != null && targets.contains(leader.id);
                announce(record);
                targets.forEach(t -> cluster.node(t).storage.setFailWrites(true));
                record.revert = () -> {
                    for (String t : targets) {
                        cluster.node(t).storage.setFailWrites(false);
                        cluster.restart(t); // operator fixes the disk and restarts the fail-stopped process
                    }
                };
            }
            case DISK_LOSS -> {
                record.description = "DISK_LOSS on " + targets;
                record.hitLeader = leader != null && targets.contains(leader.id);
                announce(record);
                for (String t : targets) {
                    cluster.wipeDisk(t);
                    checker.noteAssumptionBreach(t, t + " lost its disk at " + cluster.loop().nowMs()
                            + " ms (Raft assumes acknowledged state is never lost)");
                }
                record.revert = () -> targets.forEach(cluster::restart);
            }
            case HEAL -> {
                record.description = "HEAL all network faults";
                announce(record);
                cluster.network().healAll();
            }
        }
        faults.add(record);

        if (record.revert == null) {
            record.healedAt = record.injectedAt; // instantaneous faults
            return;
        }
        record.active = true;
        if (fault.type() == FaultType.DISK_LOSS && fault.durationMs() == null && fault.end() == null) {
            cluster.loop().schedule(0, () -> heal(record));
        } else if (fault.durationMs() != null) {
            cluster.loop().schedule(fault.durationMs(), () -> heal(record));
        } else if (fault.end() != null) {
            onTrigger(fault.end(), () -> heal(record));
        }
    }

    private void announce(MutableFault record) {
        cluster.emit(RaftEvent.Type.FAULT_INJECTED, String.join(",", record.targets), record.description);
    }

    private void heal(MutableFault record) {
        if (!record.active) return;
        record.active = false;
        record.healedAt = cluster.loop().nowMs();
        cluster.emit(RaftEvent.Type.FAULT_HEALED, String.join(",", record.targets), record.description);
        record.revert.run();
    }

    // =====================================================================================
    // Verdicts
    // =====================================================================================

    private Liveness judgeLiveness(RunMetrics metrics) {
        List<String> findings = new ArrayList<>();
        int quorum = scenario.nodes() / 2 + 1;
        // Largest set of serving nodes that can all reach one given node.
        int bestComponent = 0;
        for (SimNode a : cluster.nodes()) {
            if (!a.isServing()) continue;
            int reach = 0;
            for (SimNode b : cluster.nodes()) {
                if (b.isServing() && (a == b || cluster.network().connected(a.id, b.id))) reach++;
            }
            bestComponent = Math.max(bestComponent, reach);
        }
        boolean quorumAvailable = bestComponent >= quorum;
        SimNode leader = cluster.leader();
        boolean converged = false;
        Liveness.Status status;
        long settled = settleMs;
        if (!quorumAvailable) {
            status = Liveness.Status.EXPECTED_UNAVAILABILITY;
            findings.add("At the end of the run only " + bestComponent + " of " + scenario.nodes()
                    + " nodes could reach each other, fewer than the quorum of " + quorum
                    + ". Refusing to elect a leader or commit is the correct behaviour in that state.");
        } else if (leader == null) {
            status = Liveness.Status.ISSUE;
            findings.add("A quorum of " + bestComponent + " nodes was available, yet no leader existed "
                    + settled + " ms after the workload stopped.");
        } else {
            converged = true;
            for (SimNode n : cluster.nodes()) {
                if (n.isServing() && cluster.network().connected(leader.id, n.id)
                        && n.raft().lastApplied() != leader.raft().commitIndex()) {
                    converged = false;
                    findings.add(n.id + " had applied index " + n.raft().lastApplied() + " while the leader had committed "
                            + leader.raft().commitIndex() + ".");
                }
            }
            status = converged ? Liveness.Status.OK : Liveness.Status.ISSUE;
            findings.add(converged
                    ? "Leader " + leader.id + " (term " + leader.raft().currentTerm() + ") was in place and every reachable node "
                            + "had applied the same prefix (index " + leader.raft().commitIndex() + ") " + settled
                            + " ms after the workload stopped."
                    : "Reachable nodes had not converged " + settled + " ms after the workload stopped.");
        }
        if (metrics.failedOutsideFaults() > 0) {
            findings.add(metrics.failedOutsideFaults() + " client request(s) did not succeed although no fault was active "
                    + "(or recently healed) at the time.");
            if (status == Liveness.Status.OK) status = Liveness.Status.ISSUE;
        }
        if (metrics.failedDuringFaults() > 0) {
            findings.add(metrics.failedDuringFaults() + " client request(s) did not succeed while a fault was active or "
                    + "the cluster was recovering from one: expected unavailability.");
        }
        return new Liveness(status, quorumAvailable, leader == null ? null : leader.id, converged, findings);
    }

    private List<NodeEnd> finalState() {
        List<NodeEnd> out = new ArrayList<>();
        for (SimNode n : cluster.nodes()) {
            boolean serving = n.isServing();
            out.add(new NodeEnd(n.id, n.isUp(), serving ? n.raft().role().name() : n.isUp() ? "FAILED" : "DOWN",
                    n.disk.currentTerm(), n.disk.lastIndex(), serving ? n.raft().commitIndex() : 0,
                    serving ? n.raft().lastApplied() : 0, n.disk.snapshotIndex(), serving ? n.kv().size() : 0,
                    serving ? n.kv().digest() : 0));
        }
        return out;
    }
}
