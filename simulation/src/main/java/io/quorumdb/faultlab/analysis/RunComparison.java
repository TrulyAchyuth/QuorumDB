package io.quorumdb.faultlab.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Side-by-side comparison of two runs. */
public record RunComparison(String aName, String bName, boolean sameScenario, boolean identicalTrace,
                            List<Row> rows, List<String> observations) {

    /**
     * @param better "a", "b" or "" when equal / not comparable
     */
    public record Row(String metric, String unit, String a, String b, String delta, String better) {}

    public static RunComparison compare(RunResult a, RunResult b) {
        RunMetrics x = a.metrics();
        RunMetrics y = b.metrics();
        List<Row> rows = new ArrayList<>();
        rows.add(text("Headline", a.headline().name(), b.headline().name()));
        rows.add(text("Safety audit", a.safety().verdict().name(), b.safety().verdict().name()));
        rows.add(text("Client history", a.linearizability() == null ? "n/a" : a.linearizability().result().name(),
                b.linearizability() == null ? "n/a" : b.linearizability().result().name()));
        rows.add(text("Liveness", a.liveness().status().name(), b.liveness().status().name()));
        rows.add(num("Availability", "%", x.availability() * 100, y.availability() * 100, true));
        rows.add(num("Failed requests", "", x.opsFailed() + x.opsIndeterminate(), y.opsFailed() + y.opsIndeterminate(), false));
        rows.add(num("Failed outside fault windows", "", x.failedOutsideFaults(), y.failedOutsideFaults(), false));
        rows.add(num("Successful requests", "", x.opsOk(), y.opsOk(), true));
        rows.add(num("Latency p50", "ms", x.latencyP50Ms(), y.latencyP50Ms(), false));
        rows.add(num("Latency p99", "ms", x.latencyP99Ms(), y.latencyP99Ms(), false));
        rows.add(num("Leader changes", "", x.leaderChanges(), y.leaderChanges(), false));
        rows.add(num("Elections started", "", x.electionsStarted(), y.electionsStarted(), false));
        rows.add(num("Final term", "", x.finalTerm(), y.finalTerm(), false));
        rows.add(num("Mean election duration", "ms", x.meanElectionMs(), y.meanElectionMs(), false));
        rows.add(num("Longest leaderless window", "ms", x.maxElectionMs(), y.maxElectionMs(), false));
        rows.add(num("Mean recovery duration", "ms", x.meanRecoveryMs(), y.meanRecoveryMs(), false));
        rows.add(num("Longest recovery", "ms", x.maxRecoveryMs(), y.maxRecoveryMs(), false));
        rows.add(num("Final commit index", "", x.finalCommitIndex(), y.finalCommitIndex(), true));
        rows.add(num("Mean replication lag", "entries", x.meanReplicationLag(), y.meanReplicationLag(), false));
        rows.add(num("Max replication lag", "entries", x.maxReplicationLag(), y.maxReplicationLag(), false));
        rows.add(num("Log truncations", "", x.logTruncations(), y.logTruncations(), false));
        rows.add(num("Messages sent", "", x.messagesSent(), y.messagesSent(), false));
        rows.add(num("Messages dropped", "", x.messagesDropped(), y.messagesDropped(), false));

        List<String> notes = new ArrayList<>();
        boolean sameScenario = a.scenario().equals(b.scenario());
        boolean identical = a.traceHash().equals(b.traceHash());
        if (a.mode() != b.mode()) notes.add("The runs come from different modes (simulation vs live cluster); times are not directly comparable.");
        if (identical) notes.add("The two traces are event-for-event identical (same hash).");
        if (!a.scenario().cluster().equals(b.scenario().cluster())) {
            notes.add("Cluster settings differ: " + a.scenario().cluster() + " vs " + b.scenario().cluster() + ".");
        }
        if (!java.util.Objects.equals(a.scenario().seed(), b.scenario().seed())) {
            notes.add("Seeds differ (" + a.scenario().seed() + " vs " + b.scenario().seed() + "): timing differences may be due to different random election timeouts and latencies.");
        }
        if (a.safety().hasViolations() != b.safety().hasViolations()) {
            notes.add("Only run " + (a.safety().hasViolations() ? "A" : "B") + " violated a safety property.");
        }
        return new RunComparison(a.scenario().name(), b.scenario().name(), sameScenario, identical, rows, notes);
    }

    private static Row text(String metric, String a, String b) {
        return new Row(metric, "", a, b, a.equals(b) ? "same" : "differs", "");
    }

    private static Row num(String metric, String unit, double a, double b, boolean higherIsBetter) {
        double d = b - a;
        String better = Math.abs(d) < 1e-9 ? "" : (d > 0) == higherIsBetter ? "b" : "a";
        return new Row(metric, unit, fmt(a), fmt(b), (d > 0 ? "+" : "") + fmt(d), better);
    }

    private static String fmt(double v) {
        if (Math.abs(v - Math.rint(v)) < 1e-9) return String.valueOf((long) Math.rint(v));
        return String.format(Locale.ROOT, "%.2f", v);
    }
}
