package io.quorumdb.faultlab.analysis;

import io.quorumdb.faultlab.analysis.RunResult.Finding;
import io.quorumdb.faultlab.analysis.RunResult.Liveness;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.raft.RaftEvent.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a recorded run into a narrative of what happened and why.
 *
 * <p>This is a deterministic, rule-based summariser, not a language model: the run is cut into
 * episodes at every fault injection/heal, and each sentence is produced from events counted in
 * that episode. Every finding lists the sequence numbers of the events it rests on, so a reader
 * can jump from a claim to its evidence in the timeline. If the events are not there, the
 * sentence is not generated.
 */
public final class Explainer {
    private Explainer() {}

    private static final int MAX_EVIDENCE = 12;

    public static List<Finding> explain(List<RaftEvent> events, SafetyReport safety,
                                        LinearizabilityChecker.Report linearizability, Liveness liveness) {
        List<Finding> findings = new ArrayList<>();
        // Episode boundaries: start of run and every fault transition.
        List<Integer> cuts = new ArrayList<>();
        cuts.add(0);
        for (int i = 0; i < events.size(); i++) {
            Type t = events.get(i).type();
            if (t == Type.FAULT_INJECTED || t == Type.FAULT_HEALED) cuts.add(i);
        }
        boolean auditClean = !safety.hasViolations();
        long committedBefore = 0;
        for (int c = 0; c < cuts.size(); c++) {
            int from = cuts.get(c);
            int to = c + 1 < cuts.size() ? cuts.get(c + 1) : events.size();
            if (from >= to) continue;
            RaftEvent head = events.get(from);
            boolean isFault = head.type() == Type.FAULT_INJECTED || head.type() == Type.FAULT_HEALED;
            String title = isFault
                    ? (head.type() == Type.FAULT_INJECTED ? "Fault injected: " : "Fault healed: ") + head.detail()
                    : "Cluster start";
            List<RaftEvent> window = events.subList(isFault ? from + 1 : from, to);
            Finding f = episode(window, isFault ? head : null, title, isFault ? head.timeMs() : 0, committedBefore, auditClean);
            if (f != null) findings.add(f);
            for (RaftEvent e : window) {
                if (e.type() == Type.ENTRY_COMMITTED) committedBefore = Math.max(committedBefore, e.index());
            }
        }
        findings.add(safetyFinding(safety, events));
        if (linearizability != null) findings.add(linearizabilityFinding(linearizability, events));
        if (liveness != null) findings.add(livenessFinding(liveness, events));
        return findings;
    }

    /**
     * @param committedBefore highest index committed anywhere before this episode, so that a node merely
     *                        re-learning old commits (after a restart or rejoin) is not reported as progress
     * @param auditClean      whether the safety audit found no violation (gates claims that depend on it)
     */
    private static Finding episode(List<RaftEvent> window, RaftEvent head, String title, long startMs,
                                   long committedBefore, boolean auditClean) {
        List<Long> evidence = new ArrayList<>();
        if (head != null) evidence.add(head.seq());
        StringBuilder text = new StringBuilder();

        RaftEvent firstTimeout = null;
        int timeouts = 0, preVotes = 0, campaigns = 0, heartbeatless = 0;
        Map<String, List<RaftEvent>> denials = new LinkedHashMap<>();
        List<RaftEvent> elected = new ArrayList<>();
        List<RaftEvent> steppedDown = new ArrayList<>();
        List<RaftEvent> truncated = new ArrayList<>();
        List<RaftEvent> failures = new ArrayList<>();
        List<RaftEvent> snapshots = new ArrayList<>();
        List<RaftEvent> recoveries = new ArrayList<>();
        Map<String, Long> uncommittedAppends = new LinkedHashMap<>();
        long firstCommit = -1, lastCommit = -1;
        RaftEvent firstCommitEvent = null, lastCommitEvent = null;
        long maxTermSeen = 0, minTermSeen = Long.MAX_VALUE;
        int okOps = 0, failedOps = 0, unknownOps = 0;
        RaftEvent firstFailedOp = null;

        for (RaftEvent e : window) {
            switch (e.type()) {
                case ELECTION_TIMEOUT -> {
                    timeouts++;
                    if (firstTimeout == null) firstTimeout = e;
                }
                case PRE_VOTE_STARTED -> preVotes++;
                case ELECTION_STARTED -> campaigns++;
                case VOTE_DENIED -> denials.computeIfAbsent(reason(e.detail()), k -> new ArrayList<>()).add(e);
                case LEADER_ELECTED -> elected.add(e);
                case STEPPED_DOWN -> {
                    if (e.detail() != null && e.detail().startsWith("was LEADER")) steppedDown.add(e);
                }
                case LOG_TRUNCATED -> truncated.add(e);
                case NODE_FAILED -> failures.add(e);
                case SNAPSHOT_CREATED, SNAPSHOT_INSTALLED -> snapshots.add(e);
                case RECOVERY_COMPLETED -> recoveries.add(e);
                case ENTRY_COMMITTED -> {
                    if (e.index() <= committedBefore) break; // a node catching up on already-committed entries
                    if (firstCommit < 0) {
                        firstCommitEvent = e;
                        firstCommit = e.index();
                    }
                    if (e.index() > lastCommit) {
                        lastCommit = e.index();
                        lastCommitEvent = e;
                    }
                }
                case CLIENT_RESPONSE -> {
                    String d = e.detail() == null ? "" : e.detail();
                    if (d.startsWith("OK")) okOps++;
                    else if (d.startsWith("INDETERMINATE")) unknownOps++;
                    else {
                        failedOps++;
                        if (firstFailedOp == null) firstFailedOp = e;
                    }
                }
                default -> { }
            }
            if (e.term() > 0 && e.type() != Type.MESSAGE_SENT && e.type() != Type.MESSAGE_DELIVERED
                    && e.type() != Type.MESSAGE_DROPPED) {
                maxTermSeen = Math.max(maxTermSeen, e.term());
                minTermSeen = Math.min(minTermSeen, e.term());
            }
        }
        if (window.isEmpty()) return head == null ? null
                : new Finding("EPISODE", startMs, title, "Nothing further was recorded before the next fault transition.", evidence);

        if (firstTimeout != null) {
            text.append(firstTimeout.node()).append("'s election timer was the first to fire, at ")
                    .append(firstTimeout.timeMs()).append(" ms");
            if (head != null) text.append(" (").append(firstTimeout.timeMs() - startMs).append(" ms after this point)");
            text.append(timeouts > 1 ? "; " + timeouts + " timeouts fired in total. " : ". ");
            add(evidence, firstTimeout);
        }
        if (preVotes + campaigns > 0) {
            text.append(preVotes > 0 ? preVotes + " pre-vote round(s) and " : "").append(campaigns)
                    .append(" real election(s) were started. ");
        }
        for (Map.Entry<String, List<RaftEvent>> d : denials.entrySet()) {
            RaftEvent sample = d.getValue().get(0);
            text.append(d.getValue().size()).append(" vote request(s) were refused because: ").append(d.getKey())
                    .append(" (first: ").append(sample.node()).append(" refused ").append(sample.peer())
                    .append(" at ").append(sample.timeMs()).append(" ms). ");
            add(evidence, sample);
        }
        for (RaftEvent e : elected) {
            text.append(e.node()).append(" became leader of term ").append(e.term()).append(" at ").append(e.timeMs())
                    .append(" ms");
            if (head != null) text.append(" (").append(e.timeMs() - startMs).append(" ms after this point)");
            text.append(e.detail() == null ? ". " : ", " + e.detail() + ". ");
            add(evidence, e);
        }
        for (RaftEvent e : steppedDown) {
            text.append(e.node()).append(" gave up leadership at ").append(e.timeMs()).append(" ms: ")
                    .append(e.detail().replace("was LEADER: ", "")).append(". ");
            add(evidence, e);
        }
        for (RaftEvent e : failures) {
            text.append(e.node()).append(" fail-stopped at ").append(e.timeMs()).append(" ms (").append(e.detail())
                    .append("); it stops participating rather than risk acknowledging state it could not persist. ");
            add(evidence, e);
        }
        for (RaftEvent e : truncated) {
            text.append(e.node()).append(" removed entries from its log at ").append(e.timeMs()).append(" ms (")
                    .append(e.detail()).append(")").append(auditClean
                            ? "; the safety audit confirms none of them had been committed. "
                            : ". ");
            add(evidence, e);
        }
        for (RaftEvent e : recoveries) {
            if (head == null && e.timeMs() == 0) continue; // initial boot of an empty node: nothing to tell
            text.append(e.node()).append(" recovered from disk at ").append(e.timeMs()).append(" ms (").append(e.detail())
                    .append("). ");
            add(evidence, e);
        }
        for (RaftEvent e : snapshots) {
            text.append(e.node()).append(e.type() == Type.SNAPSHOT_CREATED ? " took a snapshot through index "
                            : " was restored from the leader's snapshot through index ").append(e.index())
                    .append(" at ").append(e.timeMs()).append(" ms. ");
            add(evidence, e);
            if (evidence.size() >= MAX_EVIDENCE) break;
        }
        if (lastCommit >= 0) {
            text.append("The commit index advanced from ").append(firstCommit - 1).append(" to ").append(lastCommit)
                    .append(" (first at ").append(firstCommitEvent.timeMs()).append(" ms on ").append(firstCommitEvent.node())
                    .append("). ");
            add(evidence, firstCommitEvent);
            add(evidence, lastCommitEvent);
        } else {
            text.append("No new entry was committed in this period");
            text.append(elected.isEmpty() && (timeouts > 0 || failedOps + unknownOps > 0)
                    ? ": no node could assemble a majority. " : ". ");
        }
        if (okOps + failedOps + unknownOps > 0) {
            text.append("Client requests: ").append(okOps).append(" succeeded, ").append(failedOps).append(" failed, ")
                    .append(unknownOps).append(" had an unknown outcome. ");
            if (firstFailedOp != null) add(evidence, firstFailedOp);
        }
        if (maxTermSeen > minTermSeen && minTermSeen != Long.MAX_VALUE) {
            text.append("Terms moved from ").append(minTermSeen).append(" to ").append(maxTermSeen).append(". ");
        }
        return new Finding("EPISODE", startMs, title, text.toString().trim(), evidence);
    }

    private static String reason(String detail) {
        if (detail == null) return "the voter had already voted or its log was more up to date";
        String d = detail.replace("pre-vote: ", "").replaceAll("pre-vote for term \\d+: ", "");
        if (d.startsWith("already voted for")) return "the voter had already voted for another candidate in that term";
        if (d.startsWith("stale term")) return "the candidate's term was stale";
        return d;
    }

    private static void add(List<Long> evidence, RaftEvent e) {
        if (e != null && evidence.size() < MAX_EVIDENCE && !evidence.contains(e.seq())) evidence.add(e.seq());
    }

    private static Finding safetyFinding(SafetyReport safety, List<RaftEvent> events) {
        long end = events.isEmpty() ? 0 : events.get(events.size() - 1).timeMs();
        StringBuilder text = new StringBuilder();
        List<Long> evidence = new ArrayList<>();
        switch (safety.verdict()) {
            case SAFE -> {
                text.append("No safety property was violated. ");
                for (SafetyReport.Check c : safety.checks()) {
                    text.append(c.name()).append(": ").append(c.status() == SafetyReport.Status.NOT_EXERCISED
                            ? "not exercised" : c.examined() + " " + c.examinedWhat()).append("; ");
                }
            }
            case INCONCLUSIVE -> text.append("No violation was observed, but the history is incomplete (")
                    .append(String.join("; ", safety.observationGaps())).append("), so safety cannot be confirmed.");
            case VIOLATION -> {
                for (SafetyReport.Check c : safety.checks()) {
                    for (SafetyReport.Violation v : c.violations()) {
                        if (evidence.size() >= MAX_EVIDENCE) break;
                        text.append(c.name()).append(" violated at ").append(v.timeMs()).append(" ms: ")
                                .append(v.message()).append(". ");
                        for (Long seq : v.evidence()) if (evidence.size() < MAX_EVIDENCE) evidence.add(seq);
                    }
                }
                if (!safety.assumptionBreaches().isEmpty()) {
                    text.append("Note: this run injected a fault that breaks an assumption Raft depends on (")
                            .append(String.join("; ", safety.assumptionBreaches()))
                            .append("). The violations are real, and are the expected consequence of that breach.");
                }
            }
        }
        return new Finding("SAFETY", end, "Safety audit: " + safety.verdict(), text.toString().trim(), evidence);
    }

    private static Finding linearizabilityFinding(LinearizabilityChecker.Report r, List<RaftEvent> events) {
        long end = events.isEmpty() ? 0 : events.get(events.size() - 1).timeMs();
        StringBuilder text = new StringBuilder();
        text.append(r.operationsChecked()).append(" client operations on ").append(r.keysChecked())
                .append(" keys were checked against a sequential register model. ");
        switch (r.result()) {
            case LINEARIZABLE -> text.append("A valid linearization exists for every key.");
            case UNDETERMINED -> text.append("The search ran out of budget for some keys; no verdict for those.");
            case NOT_LINEARIZABLE -> r.keys().stream().filter(k -> k.result() == LinearizabilityChecker.Result.NOT_LINEARIZABLE)
                    .limit(3).forEach(k -> text.append("Key '").append(k.key()).append("': ").append(k.explanation()).append(". "));
        }
        return new Finding("LINEARIZABILITY", end, "Client history: " + r.result(), text.toString().trim(), List.of());
    }

    private static Finding livenessFinding(Liveness l, List<RaftEvent> events) {
        long end = events.isEmpty() ? 0 : events.get(events.size() - 1).timeMs();
        return new Finding("LIVENESS", end, "Liveness: " + l.status(), String.join(" ", l.findings()), List.of());
    }
}
