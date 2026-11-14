package io.quorumdb.faultlab.analysis;

import io.quorumdb.faultlab.analysis.RunMetrics.Election;
import io.quorumdb.faultlab.analysis.RunMetrics.Recovery;
import io.quorumdb.faultlab.analysis.RunMetrics.Sample;
import io.quorumdb.faultlab.analysis.RunResult.FaultRecord;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Op;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Outcome;
import io.quorumdb.raft.RaftEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/** Derives metrics from a recorded run. Works on simulated and live histories alike. */
public final class RunAnalyzer {
    private RunAnalyzer() {}

    public record MessageCounts(long sent, long delivered, long dropped) {}

    /**
     * @param graceMs how long after a fault heals a failed request is still attributed to it
     */
    public static RunMetrics analyze(List<RaftEvent> events, List<FaultRecord> faults, List<Op> ops, List<Sample> samples,
                                     long durationMs, int nodes, long graceMs, MessageCounts messages) {
        // ---- client operations ----
        long ok = 0, failed = 0, indeterminate = 0, duringFaults = 0, outsideFaults = 0;
        List<Long> latencies = new ArrayList<>();
        for (Op op : ops) {
            switch (op.outcome()) {
                case OK -> {
                    ok++;
                    latencies.add(op.completeMs() - op.invokeMs());
                }
                case FAILED -> failed++;
                case INDETERMINATE -> indeterminate++;
            }
            if (op.outcome() != Outcome.OK) {
                if (overlapsFault(op, faults, graceMs, durationMs)) duringFaults++; else outsideFaults++;
            }
        }
        long total = ops.size();
        long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();

        // ---- elections ----
        List<Election> elections = new ArrayList<>();
        String currentLeader = null;
        long leaderlessSince = 0;
        long firstTimeout = -1;
        long electionsStarted = 0, truncations = 0, snapshotsCreated = 0, snapshotsInstalled = 0, finalTerm = 0;
        long finalCommit = 0;
        for (RaftEvent e : events) {
            finalTerm = Math.max(finalTerm, isCore(e) ? e.term() : 0);
            switch (e.type()) {
                case ELECTION_TIMEOUT -> {
                    if (firstTimeout < 0 && e.timeMs() >= leaderlessSince) firstTimeout = e.timeMs();
                }
                case ELECTION_STARTED -> electionsStarted++;
                case NODE_CRASHED, NODE_DISCONNECTED -> {
                    if (e.peer() != null && e.peer().equals(currentLeader)) {
                        currentLeader = null;
                        leaderlessSince = e.timeMs();
                        firstTimeout = -1;
                    }
                }
                case STEPPED_DOWN, NODE_FAILED, NODE_STOPPED -> {
                    if (e.node().equals(currentLeader)) {
                        currentLeader = null;
                        leaderlessSince = e.timeMs();
                        firstTimeout = -1;
                    }
                }
                case LEADER_ELECTED -> {
                    if (currentLeader != null) {
                        // The old leader never noticed (e.g. it sits in a minority partition): the cluster
                        // was effectively leaderless since the last fault that involved it.
                        long since = leaderlessSince;
                        for (FaultRecord f : faults) {
                            if (f.injectedAtMs() <= e.timeMs() && f.hitLeader() && f.injectedAtMs() > since) {
                                since = f.injectedAtMs();
                            }
                        }
                        leaderlessSince = since;
                        if (firstTimeout < leaderlessSince) firstTimeout = -1;
                    }
                    long timeout = firstTimeout < 0 ? e.timeMs() : firstTimeout;
                    elections.add(new Election(e.term(), e.node(), leaderlessSince, timeout, e.timeMs(),
                            e.timeMs() - leaderlessSince));
                    currentLeader = e.node();
                    firstTimeout = -1;
                }
                case LOG_TRUNCATED -> truncations++;
                case SNAPSHOT_CREATED -> snapshotsCreated++;
                case SNAPSHOT_INSTALLED -> snapshotsInstalled++;
                case ENTRY_COMMITTED -> finalCommit = Math.max(finalCommit, e.index());
                default -> { }
            }
        }
        double meanElection = elections.stream().mapToLong(Election::durationMs).average().orElse(0);
        long maxElection = elections.stream().mapToLong(Election::durationMs).max().orElse(0);

        // ---- recoveries ----
        List<Recovery> recoveries = new ArrayList<>();
        for (FaultRecord f : faults) {
            long newLeaderAfter = -1, firstCommitAfter = -1, catchUp = -1;
            if (f.hitLeader()) {
                for (RaftEvent e : events) {
                    if (e.timeMs() < f.injectedAtMs()) continue;
                    if (newLeaderAfter < 0 && e.type() == RaftEvent.Type.LEADER_ELECTED && e.term() > f.termAtInjection()) {
                        newLeaderAfter = e.timeMs() - f.injectedAtMs();
                    }
                    if (newLeaderAfter >= 0 && e.type() == RaftEvent.Type.ENTRY_COMMITTED && e.term() > f.termAtInjection()) {
                        firstCommitAfter = e.timeMs() - f.injectedAtMs();
                        break;
                    }
                }
            }
            // Catch-up: after each restart of a targeted node, when did it apply the then-committed prefix?
            long restartAt = -1, commitAtRestart = 0, maxCommit = 0;
            String restarted = null;
            for (RaftEvent e : events) {
                if (e.type() == RaftEvent.Type.ENTRY_COMMITTED) maxCommit = Math.max(maxCommit, e.index());
                if (e.timeMs() < f.injectedAtMs()) continue;
                if (restartAt < 0 && e.type() == RaftEvent.Type.NODE_RESTARTED && f.targets().contains(e.peer())) {
                    restartAt = e.timeMs();
                    commitAtRestart = maxCommit;
                    restarted = e.peer();
                } else if (restartAt >= 0 && e.node().equals(restarted) && e.index() >= commitAtRestart
                        && (e.type() == RaftEvent.Type.ENTRY_APPLIED || e.type() == RaftEvent.Type.SNAPSHOT_INSTALLED
                        || (e.type() == RaftEvent.Type.RECOVERY_COMPLETED && commitAtRestart == 0))) {
                    catchUp = e.timeMs() - restartAt;
                    break;
                }
            }
            recoveries.add(new Recovery(f.step(), f.description(), f.injectedAtMs(), newLeaderAfter, firstCommitAfter, catchUp));
        }
        long[] recoveryTimes = recoveries.stream().mapToLong(r -> Math.max(r.firstCommitAfterMs(), r.catchUpMs()))
                .filter(v -> v >= 0).toArray();
        double meanRecovery = Arrays.stream(recoveryTimes).average().orElse(0);
        long maxRecovery = Arrays.stream(recoveryTimes).max().orElse(0);

        double meanLag = samples.stream().filter(s -> s.leader() != null).mapToLong(Sample::replicationLag).average().orElse(0);
        long maxLag = samples.stream().mapToLong(Sample::replicationLag).max().orElse(0);

        return new RunMetrics(durationMs, nodes, total, ok, failed, indeterminate, total == 0 ? 1.0 : (double) ok / total,
                duringFaults, outsideFaults, percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99),
                durationMs == 0 ? 0 : ok * 1000.0 / durationMs,
                elections, meanElection, maxElection, recoveries, meanRecovery, maxRecovery,
                finalCommit, finalTerm, electionsStarted, Math.max(0, elections.size() - 1), truncations,
                snapshotsCreated, snapshotsInstalled, meanLag, maxLag,
                messages.sent(), messages.delivered(), messages.dropped(), samples);
    }

    /** One-word classification of a run; shared by simulated and live runs. */
    public static RunResult.Headline headline(io.quorumdb.faultlab.check.SafetyReport safety,
                                              io.quorumdb.faultlab.check.LinearizabilityChecker.Report lin,
                                              RunResult.Liveness liveness, RunMetrics metrics) {
        if (safety.hasViolations()
                || (lin != null && lin.result() == io.quorumdb.faultlab.check.LinearizabilityChecker.Result.NOT_LINEARIZABLE)) {
            return RunResult.Headline.SAFETY_VIOLATION;
        }
        if (safety.verdict() == io.quorumdb.faultlab.check.SafetyReport.Verdict.INCONCLUSIVE) {
            return RunResult.Headline.INCONCLUSIVE;
        }
        if (liveness.status() == RunResult.Liveness.Status.ISSUE) return RunResult.Headline.LIVENESS_ISSUE;
        if (liveness.status() == RunResult.Liveness.Status.EXPECTED_UNAVAILABILITY || metrics.failedDuringFaults() > 0) {
            return RunResult.Headline.EXPECTED_UNAVAILABILITY;
        }
        return RunResult.Headline.HEALTHY;
    }

    private static boolean isCore(RaftEvent e) {
        return switch (e.type()) {
            case MESSAGE_SENT, MESSAGE_DELIVERED, MESSAGE_DROPPED -> false;
            default -> true;
        };
    }

    static boolean overlapsFault(Op op, List<FaultRecord> faults, long graceMs, long durationMs) {
        long end = op.completeMs() == Long.MAX_VALUE ? durationMs : op.completeMs();
        for (FaultRecord f : faults) {
            long healed = f.healedAtMs() < 0 ? Long.MAX_VALUE - graceMs : f.healedAtMs();
            if (op.invokeMs() <= healed + graceMs && end >= f.injectedAtMs()) return true;
        }
        return false;
    }

    public static double percentile(long[] sorted, double q) {
        if (sorted.length == 0) return 0;
        int rank = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
    }

    /** Fingerprint of an event trace; equal hashes mean the two runs were event-for-event identical. */
    public static String traceHash(List<RaftEvent> events) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            StringBuilder line = new StringBuilder(128);
            for (RaftEvent e : events) {
                line.setLength(0);
                line.append(e.seq()).append('|').append(e.timeMs()).append('|').append(e.node()).append('|')
                        .append(e.type()).append('|').append(e.term()).append('|').append(e.index()).append('|')
                        .append(e.entryTerm()).append('|').append(e.checksum()).append('|').append(e.peer()).append('|')
                        .append(e.detail()).append('\n');
                sha.update(line.toString().getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
