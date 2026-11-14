package io.quorumdb.faultlab.check;

import io.quorumdb.faultlab.check.SafetyReport.Check;
import io.quorumdb.faultlab.check.SafetyReport.Status;
import io.quorumdb.faultlab.check.SafetyReport.Violation;
import io.quorumdb.raft.RaftEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Independent safety auditor. It never looks inside {@code RaftNode}; it only consumes the
 * recorded event history plus periodic {@link NodeObservation}s of each node's log, and checks
 * them against the properties Raft promises (paper Figure 3) and their consequences.
 *
 * <table>
 * <tr><th>Check</th><th>Property</th></tr>
 * <tr><td>ELECTION_SAFETY</td><td>At most one leader is elected in a given term.</td></tr>
 * <tr><td>LOG_MATCHING</td><td>If two logs contain an entry with the same index and term, the logs are
 *     identical in all entries up through that index.</td></tr>
 * <tr><td>LEADER_COMPLETENESS</td><td>An entry committed in some term is present in the log of every leader of
 *     a later term.</td></tr>
 * <tr><td>COMMIT_AGREEMENT</td><td>No two nodes ever commit different entries at the same index.</td></tr>
 * <tr><td>COMMIT_DURABILITY</td><td>A committed entry never disappears from, or changes in, the log of a node
 *     that considers it committed.</td></tr>
 * <tr><td>COMMIT_PROGRESSION</td><td>A node's commit index only moves forward, one entry after another.</td></tr>
 * <tr><td>STATE_MACHINE_SAFETY</td><td>Each node applies entries in index order, only after they are committed,
 *     and no two nodes apply different entries at the same index; replicas that applied the same prefix
 *     have identical state.</td></tr>
 * <tr><td>TERM_MONOTONICITY</td><td>A node's current term never decreases, not even across restarts.</td></tr>
 * </table>
 *
 * <p>Unavailability (no leader, failed requests) is deliberately <em>not</em> judged here: it is a
 * liveness matter, reported separately.
 */
public final class SafetyChecker {

    private record Committed(long entryTerm, long checksum, long committedInTerm, String node, long seq) {}

    private static final int MAX_VIOLATIONS_PER_CHECK = 25;

    private final Map<String, List<Violation>> violations = new LinkedHashMap<>();
    private final Map<String, Long> examined = new HashMap<>();
    private final List<String> assumptionBreaches = new ArrayList<>();
    private final List<String> gaps = new ArrayList<>();

    private final Map<Long, RaftEvent> leaderByTerm = new HashMap<>();
    private final TreeMap<Long, Committed> committed = new TreeMap<>();
    private final Map<Long, long[]> appliedAt = new HashMap<>(); // index -> {entryTerm, checksum, seq}
    private final Map<String, Long> nodeCommit = new HashMap<>();
    private final Map<String, Long> nodeApplied = new HashMap<>();
    private final Map<String, Long> nodeTerm = new HashMap<>();
    private final Map<String, Long> nodeSeq = new HashMap<>();
    private final Map<String, long[]> entryByIndexTerm = new HashMap<>(); // "index@term" -> {checksum, seq}
    private final Map<Long, long[]> digestAtApplied = new HashMap<>();   // lastApplied -> {digest, time}
    private final Map<Long, String> digestNode = new HashMap<>();
    private final Map<String, NodeObservation> latest = new LinkedHashMap<>();
    private final boolean perNodeSequences;

    /**
     * @param perNodeSequences true when events carry per-node sequence numbers (live clusters); gaps in a
     *                         node's sequence then mark the audit as incomplete
     */
    public SafetyChecker(boolean perNodeSequences) {
        this.perNodeSequences = perNodeSequences;
        for (String id : List.of("ELECTION_SAFETY", "LOG_MATCHING", "LEADER_COMPLETENESS", "COMMIT_AGREEMENT",
                "COMMIT_DURABILITY", "COMMIT_PROGRESSION", "STATE_MACHINE_SAFETY", "TERM_MONOTONICITY")) {
            violations.put(id, new ArrayList<>());
            examined.put(id, 0L);
        }
    }

    // =====================================================================================
    // Inputs
    // =====================================================================================

    /** Declares that a fault broke one of Raft's assumptions (e.g. durable storage was lost). */
    public void noteAssumptionBreach(String node, String description) {
        assumptionBreaches.add(description);
        if (node != null) {
            nodeTerm.remove(node); // its term restarts from whatever is (not) on the replaced disk
        }
    }

    public void noteObservationGap(String description) {
        gaps.add(description);
    }

    public void onEvent(RaftEvent e) {
        String node = e.node();
        if (perNodeSequences) {
            Long previous = nodeSeq.put(node, e.seq());
            if (previous != null && e.seq() > previous + 1) {
                gaps.add("events " + (previous + 1) + ".." + (e.seq() - 1) + " of " + node + " were not observed");
                // We can no longer follow this node's commit/apply sequence exactly.
                nodeCommit.remove(node);
                nodeApplied.remove(node);
            }
        }
        boolean fromRaftCore = switch (e.type()) {
            // Message events carry the message's term (a pre-vote names a term its sender has not entered).
            case MESSAGE_SENT, MESSAGE_DELIVERED, MESSAGE_DROPPED, NODE_CRASHED, NODE_RESTARTED, NODE_DISCONNECTED,
                 NODE_RECOVERED, FAULT_INJECTED, FAULT_HEALED, CLIENT_REQUEST, CLIENT_RESPONSE -> false;
            default -> true;
        };
        if (!fromRaftCore) return;
        if (e.term() > 0) {
            count("TERM_MONOTONICITY");
            Long last = nodeTerm.get(node);
            if (last != null && e.term() < last) {
                violate("TERM_MONOTONICITY", node + " went back from term " + last + " to term " + e.term(),
                        e, List.of(node), 0, e.term(), List.of(e.seq()));
            }
            nodeTerm.merge(node, e.term(), Math::max);
        }
        switch (e.type()) {
            case RECOVERY_COMPLETED, SNAPSHOT_INSTALLED -> {
                // After a restart (or snapshot install) commit and apply resume from the snapshot point.
                nodeCommit.put(node, e.index());
                nodeApplied.put(node, e.index());
            }
            case LEADER_ELECTED -> onLeaderElected(e);
            case ENTRY_APPENDED -> onAppended(e);
            case ENTRY_COMMITTED -> onCommitted(e);
            case ENTRY_APPLIED -> onApplied(e);
            default -> { }
        }
    }

    private void onLeaderElected(RaftEvent e) {
        count("ELECTION_SAFETY");
        RaftEvent other = leaderByTerm.putIfAbsent(e.term(), e);
        if (other != null && !other.node().equals(e.node())) {
            violate("ELECTION_SAFETY", "two leaders in term " + e.term() + ": " + other.node() + " and " + e.node(),
                    e, List.of(other.node(), e.node()), 0, e.term(), List.of(other.seq(), e.seq()));
        }
    }

    private void onAppended(RaftEvent e) {
        count("LOG_MATCHING");
        String key = e.index() + "@" + e.entryTerm();
        long[] known = entryByIndexTerm.putIfAbsent(key, new long[] {e.checksum(), e.seq()});
        if (known != null && known[0] != e.checksum()) {
            violate("LOG_MATCHING", "two different entries exist with index " + e.index() + " and term " + e.entryTerm(),
                    e, List.of(e.node()), e.index(), e.entryTerm(), List.of(known[1], e.seq()));
        }
    }

    private void onCommitted(RaftEvent e) {
        String node = e.node();
        count("COMMIT_AGREEMENT");
        Committed first = committed.get(e.index());
        if (first == null) {
            committed.put(e.index(), new Committed(e.entryTerm(), e.checksum(), e.term(), node, e.seq()));
        } else {
            if (first.entryTerm() != e.entryTerm() || first.checksum() != e.checksum()) {
                violate("COMMIT_AGREEMENT", "index " + e.index() + " was committed with different entries: "
                                + first.node() + " committed term-" + first.entryTerm() + " entry, " + node
                                + " committed term-" + e.entryTerm() + " entry",
                        e, List.of(first.node(), node), e.index(), e.entryTerm(), List.of(first.seq(), e.seq()));
            }
            if (e.term() < first.committedInTerm()) {
                committed.put(e.index(), new Committed(first.entryTerm(), first.checksum(), e.term(), first.node(), first.seq()));
            }
        }
        Long previous = nodeCommit.get(node);
        if (previous != null) {
            count("COMMIT_PROGRESSION");
            if (e.index() != previous + 1) {
                violate("COMMIT_PROGRESSION", node + " moved its commit index from " + previous + " to " + e.index(),
                        e, List.of(node), e.index(), e.term(), List.of(e.seq()));
            }
        }
        nodeCommit.put(node, e.index());
    }

    private void onApplied(RaftEvent e) {
        String node = e.node();
        count("STATE_MACHINE_SAFETY");
        Long previous = nodeApplied.get(node);
        if (previous != null && e.index() != previous + 1) {
            violate("STATE_MACHINE_SAFETY", node + " applied index " + e.index() + " right after index " + previous
                    + " (must apply in log order, exactly once)", e, List.of(node), e.index(), e.term(), List.of(e.seq()));
        }
        nodeApplied.put(node, e.index());
        Long commitIndex = nodeCommit.get(node);
        if (commitIndex != null && e.index() > commitIndex) {
            violate("STATE_MACHINE_SAFETY", node + " applied index " + e.index() + " before committing it (commit index "
                    + commitIndex + ")", e, List.of(node), e.index(), e.term(), List.of(e.seq()));
        }
        long[] first = appliedAt.putIfAbsent(e.index(), new long[] {e.entryTerm(), e.checksum(), e.seq()});
        if (first != null && (first[0] != e.entryTerm() || first[1] != e.checksum())) {
            violate("STATE_MACHINE_SAFETY", "different entries were applied at index " + e.index(),
                    e, List.of(node), e.index(), e.entryTerm(), List.of(first[2], e.seq()));
        }
    }

    /** Feeds a sampled view of a node's log. Call with all nodes of one sampling round. */
    public void onObservations(List<NodeObservation> round) {
        for (NodeObservation o : round) {
            latest.put(o.node(), o);
            checkObservation(o);
        }
        // Log Matching, prefix form: compare every pair of logs sampled in this round.
        for (int a = 0; a < round.size(); a++) {
            for (int b = a + 1; b < round.size(); b++) {
                checkPrefix(round.get(a), round.get(b));
            }
        }
    }

    private void checkObservation(NodeObservation o) {
        for (NodeObservation.LogRef ref : o.log()) {
            count("LOG_MATCHING");
            String key = ref.index() + "@" + ref.term();
            long[] known = entryByIndexTerm.putIfAbsent(key, new long[] {ref.checksum(), 0});
            if (known != null && known[0] != ref.checksum()) {
                violate("LOG_MATCHING", "two different entries exist with index " + ref.index() + " and term " + ref.term(),
                        o.timeMs(), List.of(o.node()), ref.index(), ref.term(), known[1] > 0 ? List.of(known[1]) : List.of());
            }
        }
        if (!o.reachable()) return;
        // Commit durability: everything this node regards as committed must still be the committed entry.
        Map<Long, Committed> regardedCommitted = o.commitIndex() > o.snapshotIndex()
                ? committed.subMap(o.snapshotIndex() + 1, true, o.commitIndex(), true) : Map.of();
        for (Map.Entry<Long, Committed> c : regardedCommitted.entrySet()) {
            count("COMMIT_DURABILITY");
            NodeObservation.LogRef ref = o.at(c.getKey());
            Committed want = c.getValue();
            if (ref == null || ref.term() != want.entryTerm() || ref.checksum() != want.checksum()) {
                violate("COMMIT_DURABILITY", "committed entry " + c.getKey() + " (term " + want.entryTerm() + ") "
                                + (ref == null ? "is missing from" : "was replaced in") + " the log of " + o.node(),
                        o.timeMs(), List.of(o.node()), c.getKey(), want.entryTerm(), List.of(want.seq()));
            }
        }
        if ("LEADER".equals(o.role())) {
            // Leader completeness: entries committed in earlier terms must be in this leader's log.
            for (Map.Entry<Long, Committed> c : committed.tailMap(o.snapshotIndex(), false).entrySet()) {
                Committed want = c.getValue();
                if (want.committedInTerm() >= o.term()) continue;
                count("LEADER_COMPLETENESS");
                NodeObservation.LogRef ref = o.at(c.getKey());
                if (ref == null || ref.term() != want.entryTerm() || ref.checksum() != want.checksum()) {
                    List<Long> evidence = new ArrayList<>(List.of(want.seq()));
                    RaftEvent elected = leaderByTerm.get(o.term());
                    if (elected != null) evidence.add(elected.seq());
                    violate("LEADER_COMPLETENESS", "leader " + o.node() + " of term " + o.term() + " lacks entry "
                                    + c.getKey() + " that was committed in term " + want.committedInTerm(),
                            o.timeMs(), List.of(o.node(), want.node()), c.getKey(), o.term(), evidence);
                }
            }
        }
        // Replicas that have applied exactly the same prefix must hold exactly the same data.
        if (o.lastApplied() > 0) {
            count("STATE_MACHINE_SAFETY");
            long[] known = digestAtApplied.putIfAbsent(o.lastApplied(), new long[] {o.stateDigest(), o.timeMs()});
            if (known == null) {
                digestNode.put(o.lastApplied(), o.node());
            } else if (known[0] != o.stateDigest()) {
                violate("STATE_MACHINE_SAFETY", "state divergence: " + o.node() + " and " + digestNode.get(o.lastApplied())
                                + " both applied through index " + o.lastApplied() + " but hold different data",
                        o.timeMs(), List.of(o.node(), digestNode.get(o.lastApplied())), o.lastApplied(), o.term(), List.of());
            }
        }
    }

    private void checkPrefix(NodeObservation a, NodeObservation b) {
        long from = Math.max(a.snapshotIndex(), b.snapshotIndex()) + 1;
        long to = Math.min(a.lastIndex(), b.lastIndex());
        long highestMatch = 0;
        for (long i = to; i >= from; i--) {
            if (a.at(i).term() == b.at(i).term()) {
                highestMatch = i;
                break;
            }
        }
        for (long i = from; i < highestMatch; i++) {
            count("LOG_MATCHING");
            NodeObservation.LogRef x = a.at(i);
            NodeObservation.LogRef y = b.at(i);
            if (x.term() != y.term() || x.checksum() != y.checksum()) {
                violate("LOG_MATCHING", a.node() + " and " + b.node() + " agree on entry " + highestMatch
                                + " but differ at earlier index " + i, a.timeMs(), List.of(a.node(), b.node()), i,
                        x.term(), List.of());
                return;
            }
        }
    }

    // =====================================================================================
    // Report
    // =====================================================================================

    public SafetyReport report() {
        List<Check> checks = new ArrayList<>();
        checks.add(check("ELECTION_SAFETY", "Election Safety", "At most one leader is elected in a given term.",
                "leader elections", false));
        checks.add(check("LOG_MATCHING", "Log Matching", "If two logs contain an entry with the same index and term, "
                + "the logs are identical in all entries up through that index.", "log entries compared", true));
        checks.add(check("LEADER_COMPLETENESS", "Leader Completeness", "An entry committed in some term is present in "
                + "the log of every leader of a later term.", "committed entries checked against later leaders", true));
        checks.add(check("COMMIT_AGREEMENT", "Commit Agreement", "No two nodes commit different entries at the same "
                + "log index.", "commit events", false));
        checks.add(check("COMMIT_DURABILITY", "Commit Durability", "A committed entry never disappears from or changes "
                + "in the log of a node that considers it committed.", "committed entries re-verified in sampled logs", true));
        checks.add(check("COMMIT_PROGRESSION", "Commit Progression", "A node's commit index only advances, one entry "
                + "after another.", "commit index advances", false));
        checks.add(check("STATE_MACHINE_SAFETY", "State Machine Safety", "Nodes apply entries in log order, only after "
                + "commit; no two nodes apply different entries at an index; equal prefixes give equal state.",
                "apply events and state digests", false));
        checks.add(check("TERM_MONOTONICITY", "Term Monotonicity", "A node's current term never decreases, even across "
                + "restarts (it is persisted before use).", "term observations", false));

        boolean violated = checks.stream().anyMatch(c -> c.status() == Status.VIOLATED);
        boolean inconclusive = !gaps.isEmpty() || checks.stream().anyMatch(c -> c.status() == Status.INCONCLUSIVE);
        SafetyReport.Verdict verdict = violated ? SafetyReport.Verdict.VIOLATION
                : inconclusive ? SafetyReport.Verdict.INCONCLUSIVE : SafetyReport.Verdict.SAFE;
        return new SafetyReport(verdict, checks, List.copyOf(assumptionBreaches), List.copyOf(gaps));
    }

    private Check check(String id, String name, String property, String what, boolean needsLogObservations) {
        List<Violation> found = violations.get(id);
        long n = examined.get(id);
        Status status;
        if (!found.isEmpty()) {
            status = Status.VIOLATED;
        } else if (!gaps.isEmpty() || (needsLogObservations && latest.values().stream().anyMatch(o -> !o.reachable())
                && perNodeSequences)) {
            // Live audit with missing events or unreachable nodes: no violation seen, but we did not see everything.
            status = Status.INCONCLUSIVE;
        } else {
            status = n == 0 ? Status.NOT_EXERCISED : Status.PASS;
        }
        return new Check(id, name, property, status, n, what, List.copyOf(found));
    }

    private void count(String check) {
        examined.merge(check, 1L, Long::sum);
    }

    private void violate(String check, String message, RaftEvent at, List<String> nodes, long index, long term,
                         List<Long> evidence) {
        violate(check, message, at.timeMs(), nodes, index, term, evidence);
    }

    private void violate(String check, String message, long timeMs, List<String> nodes, long index, long term,
                         List<Long> evidence) {
        List<Violation> list = violations.get(check);
        boolean alreadyReported = list.stream().anyMatch(v -> v.message().equals(message));
        if (!alreadyReported && list.size() < MAX_VIOLATIONS_PER_CHECK) {
            list.add(new Violation(message, timeMs, nodes, index, term, evidence));
        }
    }

    public boolean hasViolations() {
        return violations.values().stream().anyMatch(v -> !v.isEmpty());
    }
}
